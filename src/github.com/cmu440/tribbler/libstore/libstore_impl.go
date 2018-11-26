package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type libstore struct {
	mux sync.Mutex // mutex for synchronizing counter and cache

	masterServerHostPort string // the master storage server's host:port
	myHostPort           string // this Libstore's host:port (callback address sserver will use)

	leaseMode LeaseMode // a debugging flag that determines how the Libstore should request/handle leases
	// also determine whether or not the Libstore should register to receive RPCs
	// from the storage servers
	queryCacheSeconds int         // time period for tracking queries and determining whether to request leases
	queryCacheThresh  int         // the thresh for requesting leases in a period of queryCacheSeconds
	counterTimer      *time.Timer // the timer for lease counting clear epoch

	listCount map[string]int // the counter for a key called by GetList
	itemCount map[string]int // the counter for a key called by Get

	listCache map[string][]string // the cache for GetList
	itemCache map[string]string   // the cache for Get

	client         *rpc.Client            // the connection to the master
	keyClientMap   map[string]*rpc.Client // map a key to a storage server RPC client
	storageServers []storagerpc.Node      // all the storage server nodes
	virtualIDRing  map[uint32]string      // the virtualIDRing for all storage servers
}

func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	lServer := new(libstore)
	lServer.masterServerHostPort = masterServerHostPort
	lServer.myHostPort = myHostPort
	lServer.leaseMode = mode
	lServer.queryCacheSeconds = storagerpc.QueryCacheSeconds
	lServer.queryCacheThresh = storagerpc.QueryCacheThresh
	lServer.listCount = make(map[string]int)
	lServer.itemCount = make(map[string]int)
	lServer.listCache = make(map[string][]string)
	lServer.itemCache = make(map[string]string)
	lServer.keyClientMap = make(map[string]*rpc.Client)
	lServer.virtualIDRing = make(map[uint32]string)

	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	lServer.client = cli

	// call the master storage server to get a list of all the storage servers
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply

	lServer.client.Call("StorageServer.GetServers", args, &reply)

	// recall on NotReady status
	if reply.Status == storagerpc.NotReady {
		count := 0
		for count <= 5 {
			time.Sleep(time.Duration(1) * time.Second)
			lServer.client.Call("StorageServer.GetServers", args, &reply)
			if reply.Status == storagerpc.NotReady {
				count += 1
			} else {
				break
			}
		}
	}

	// if still not ready, return with error
	if reply.Status == storagerpc.NotReady {
		return nil, errors.New("storage server not ready")
	}

	// store all the storage servers
	lServer.storageServers = reply.Servers

	// build the virualID ring for all storage seervers
	lServer.BuildRing()

	// Wrap the libStore before registering it for RPC.
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(lServer))
	if err != nil {
		return nil, err
	}

	go lServer.LibStoreMain()

	//fmt.Println("return a new libstore")
	return lServer, nil
}

// the main libstore function to receive timer signals
func (ls *libstore) LibStoreMain() {
	ls.SetCounterTimer()

	for {
		select {
		case <-ls.GetCounterTimerSignal():
			ls.ClearItemCount()
			ls.ClearListCount()
			ls.ResetCounterTimer()
		}
	}
}

// if key does not exist, return non-nil error
func (ls *libstore) Get(key string) (string, error) {
	client := ls.GetClient(key)
	var nilString string
	if client == nil {
		return nilString, errors.New("cannot connect to a nil clinet")
	}

	value, hasKey := ls.GetCacheItem(key)
	leaseMode := ls.GetLeaseMode()

	// if cached, read from cache
	if hasKey && leaseMode != Never {
		return value, nil
	} else { // if not cached, request to storage server
		// set lease mode
		var mode bool
		if leaseMode == Never {
			mode = false
		} else if leaseMode == Always {
			mode = true
		} else {
			// increase the get count by one
			ls.IncreaseItemCount(key)
			// get leaseMode
			mode = ls.IfRequestItemLease(key)

			// clear the count when the mode is true
			if mode == true {
				ls.ClearItemCountByKey(key)
			}
		}

		args := &storagerpc.GetArgs{Key: key, WantLease: mode, HostPort: ls.myHostPort}
		var reply storagerpc.GetReply

		err := client.Call("StorageServer.Get", args, &reply)

		if reply.Status != storagerpc.OK {
			err = errors.New("Get not ok")
		} else {
			// if reply ok and reply lease granted, cache the returned value
			if reply.Lease.Granted {
				ls.CacheItem(key, reply.Value)
				go ls.ManageLeaseTimeout(key, 2, reply.Lease.ValidSeconds)
			}
		}

		return reply.Value, err
	}
}

// if inserting key and value succeed, return nil error
func (ls *libstore) Put(key, value string) error {
	client := ls.GetClient(key)
	if client == nil {
		return errors.New("cannot connect to a nil clinet")
	}

	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply

	err := client.Call("StorageServer.Put", args, &reply)

	if reply.Status != storagerpc.OK {
		//fmt.Println("put status", reply.Status)
		err = errors.New("Put not ok")
	}

	return err
}

func (ls *libstore) Delete(key string) error {
	client := ls.GetClient(key)
	if client == nil {
		return errors.New("cannot connect to a nil clinet")
	}

	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply

	err := client.Call("StorageServer.Delete", args, &reply)

	if reply.Status != storagerpc.OK {
		err = errors.New("Delete not ok")
	}

	return err
}

// if key does not exist, return non-nil error
func (ls *libstore) GetList(key string) ([]string, error) {
	client := ls.GetClient(key)
	var nilStrings []string
	if client == nil {
		return nilStrings, errors.New("cannot connect to a nil clinet")
	}

	value, hasKey := ls.GetCacheList(key)
	leaseMode := ls.GetLeaseMode()

	// if cached, read from cache
	if hasKey && leaseMode != Never {
		return value, nil
	} else { // if not cached, request to storage server
		// set lease mode
		var mode bool
		if leaseMode == Never {
			mode = false
		} else if leaseMode == Always {
			mode = true
		} else {
			// increase getList count by 1
			ls.IncreaseListCount(key)
			// get leaseMode
			mode = ls.IfRequestListLease(key)

			// clear the count when the mode is true
			if mode == true {
				ls.ClearListCountByKey(key)
			}
		}

		args := &storagerpc.GetArgs{Key: key, WantLease: mode, HostPort: ls.myHostPort}
		var reply storagerpc.GetListReply

		err := client.Call("StorageServer.GetList", args, &reply)

		if reply.Status != storagerpc.OK {
			err = errors.New("GetList not ok")
		} else {
			// if reply ok and mode is true, cache the returned value
			if reply.Lease.Granted {
				ls.CacheList(key, reply.Value)
				go ls.ManageLeaseTimeout(key, 1, reply.Lease.ValidSeconds)
			}
		}

		return reply.Value, err
	}
}

// if removeItem dose not exist, return non-nil error
func (ls *libstore) RemoveFromList(key, removeItem string) error {
	client := ls.GetClient(key)
	if client == nil {
		return errors.New("cannot connect to a nil clinet")
	}

	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply

	err := client.Call("StorageServer.RemoveFromList", args, &reply)

	// if the item could not be found, return an error
	if reply.Status == storagerpc.ItemNotFound {
		return errors.New(strconv.Itoa(int(reply.Status)))
	}

	return err
}

// if newItem exists in the list, return non-nil error
func (ls *libstore) AppendToList(key, newItem string) error {
	client := ls.GetClient(key)
	if client == nil {
		return errors.New("cannot connect to a nil clinet")
	}

	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply

	err := client.Call("StorageServer.AppendToList", args, &reply)

	if reply.Status != storagerpc.OK {
		return errors.New(strconv.Itoa(int(reply.Status)))
	}

	return err
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	key := args.Key
	_, hasListCache := ls.GetCacheList(key)
	_, hasItemCache := ls.GetCacheItem(key)

	if !hasListCache && !hasItemCache {
		// key is not found in either listCache or ItemCache
		reply.Status = storagerpc.KeyNotFound
	} else {
		if hasListCache {
			// revoke a listCache
			ls.RemoveCacheList(key)
			ls.ClearListCountByKey(key)
			reply.Status = storagerpc.OK
		}

		if hasItemCache {
			// revoke a ItemCache
			ls.RemoveCacheItem(key)
			ls.ClearItemCountByKey(key)
			reply.Status = storagerpc.OK
		}
	}
	return nil
}

// ==============================================
// Part 1: helper functions for lease requests

// increase the counter for GetList() corresponding to a key by 1
func (ls *libstore) IncreaseListCount(key string) {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	ls.listCount[key] = ls.listCount[key] + 1
}

// clear the counter for GetList() corresponding to all keys
func (ls *libstore) ClearListCount() {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	for key := range ls.listCount {
		ls.listCount[key] = 0
	}
}

// clear the counter for GetList() corresponding to the given key
func (ls *libstore) ClearListCountByKey(key string) {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	ls.listCount[key] = 0
}

// determine if a lease should be requested for a key called by GetList()
func (ls *libstore) IfRequestListLease(key string) bool {
	ls.mux.Lock()
	defer ls.mux.Unlock()
	ifRequest := false

	if ls.listCount[key] >= ls.queryCacheThresh {
		ifRequest = true
	}

	return ifRequest
}

// increase the counter for Get() corresponding to a key by 1
func (ls *libstore) IncreaseItemCount(key string) {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	ls.itemCount[key] = ls.itemCount[key] + 1
}

// clear the counter for Get() corresponding to all keys
func (ls *libstore) ClearItemCount() {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	for key := range ls.itemCount {
		ls.itemCount[key] = 0
	}
}

// clear the counter for Get() corresponding to the given key
func (ls *libstore) ClearItemCountByKey(key string) {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	ls.itemCount[key] = 0
}

// determine if a lease should be requested for a key called by Get()
func (ls *libstore) IfRequestItemLease(key string) bool {
	ls.mux.Lock()
	defer ls.mux.Unlock()
	ifRequest := false

	if ls.itemCount[key] >= ls.queryCacheThresh {
		ifRequest = true
	}

	return ifRequest
}

// set the timer for clearing counters(all the counters use this timer)
func (ls *libstore) SetCounterTimer() {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	ls.counterTimer = time.NewTimer(time.Duration(ls.queryCacheSeconds) * time.Second)
}

// reset the counter timer each epoch
func (ls *libstore) ResetCounterTimer() {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	ls.counterTimer.Reset(time.Duration(ls.queryCacheSeconds) * time.Second)
}

// get the signal from counter timer
func (ls *libstore) GetCounterTimerSignal() <-chan time.Time {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	return ls.counterTimer.C
}

// start the routine to manage a key's lease timeout
func (ls *libstore) ManageLeaseTimeout(key string, cacheType int, validSeconds int) {
	// start the leaseTimer
	leaseTimer := ls.SetLeaseTimer(validSeconds)

	// receive the timeout signal and delete the cached list/item corresponding to the key
	for {
		select {
		case <-ls.GetLeaseTimerSignal(leaseTimer):
			if cacheType == 1 {
				ls.RemoveCacheList(key)
			} else {
				ls.RemoveCacheItem(key)
			}
		}
	}

}

// set the timer for lease timeout(each lease/key has one of this kind of timer), each timer is only used once
// cacheType = 1 refers to list cache, cacheType = 2 refers to item cache
func (ls *libstore) SetLeaseTimer(validSeconds int) *time.Timer {
	leaseTimer := time.NewTimer(time.Duration(validSeconds) * time.Second)
	return leaseTimer
}

// get the signal from lease timer
func (ls *libstore) GetLeaseTimerSignal(leaseTimer *time.Timer) <-chan time.Time {
	return leaseTimer.C
}

// get leaseMode
func (ls *libstore) GetLeaseMode() LeaseMode {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	return ls.leaseMode
}

// ==============================================
// Part 2: helper functions for cache structures

// cache a list for a key. return ItemExists if the list for the key is already cached, else return ok
func (ls *libstore) CacheList(key string, value []string) storagerpc.Status {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	_, hasKey := ls.listCache[key]

	if hasKey {
		// already cached the list of this key, return ItemExists
		return storagerpc.ItemExists
	} else {
		// not yet cached the list of this key, cache it and return ok
		ls.listCache[key] = value
		return storagerpc.OK
	}
}

// get the cached list by key, return the cached list and whether it has been cached
func (ls *libstore) GetCacheList(key string) ([]string, bool) {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	value, hasKey := ls.listCache[key]
	return value, hasKey
}

// remove list cache for a key. return KeyNotFound if the list for the key is not cached, else return ok
func (ls *libstore) RemoveCacheList(key string) storagerpc.Status {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	_, hasKey := ls.listCache[key]

	if hasKey {
		// found the key, remove the list of this key and return ok
		delete(ls.listCache, key)
		return storagerpc.OK
	} else {
		// cannot find the key, return KeyNotFound
		return storagerpc.KeyNotFound
	}
}

// cache an item for a key. return ItemExists if the item for the key is already cached, else return ok
func (ls *libstore) CacheItem(key string, value string) storagerpc.Status {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	_, hasKey := ls.itemCache[key]

	if hasKey {
		// already cached the item of this key, return ItemExists
		return storagerpc.ItemExists
	} else {
		// not yet cached the item of this key, cache it and return ok
		ls.itemCache[key] = value
		return storagerpc.OK
	}
}

// get the cached item by key, return the cached item and whether it has been cached
func (ls *libstore) GetCacheItem(key string) (string, bool) {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	value, hasKey := ls.itemCache[key]
	return value, hasKey
}

// remove an item cache for a key. return KeyNotFound if the item for the key is not cached, else return ok
func (ls *libstore) RemoveCacheItem(key string) storagerpc.Status {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	_, hasKey := ls.itemCache[key]

	if hasKey {
		// found the key, remove the item of this key and return ok
		delete(ls.itemCache, key)
		return storagerpc.OK
	} else {
		// cannot find the key, return KeyNotFound
		return storagerpc.KeyNotFound
	}
}

// ==============================================
// Part 3: helper functions for storage server routing

// build the virtualID ring for all storage servers and all of their virtual nodes
func (ls *libstore) BuildRing() {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	for _, node := range ls.storageServers {
		for _, id := range node.VirtualIDs {
			//fmt.Println("ring vID is", id, len(node.VirtualIDs))
			ls.virtualIDRing[id] = node.HostPort
		}
	}
}

// get the RPC client corresponding to a key
func (ls *libstore) GetClient(key string) *rpc.Client {
	ls.mux.Lock()
	client, hasKey := ls.keyClientMap[key]
	ls.mux.Unlock()

	// if the corresponding client is in the cache, use it
	if hasKey {
		return client
	} else { // if the corresponding client is not in the cache, map and cache it
		client = ls.CacheKeyClientMap(key)
		return client
	}
}

// map and cache the key-client for future use
func (ls *libstore) CacheKeyClientMap(key string) *rpc.Client {
	ls.mux.Lock()
	defer ls.mux.Unlock()

	vID := StoreHash(key)

	//fmt.Println("requestID", vID)

	// find the corresponding storage server in the list
	hostPort := ls.FindNearestNode(vID)

	// build clientConn
	cli, err := rpc.DialHTTP("tcp", hostPort)
	if err != nil {
		return nil
	}

	// cache the clientConn and return the client
	ls.keyClientMap[key] = cli
	return cli
}

// find the hostPort with the nearest vID which is equal or greater than the given ID
func (ls *libstore) FindNearestNode(vID uint32) string {
	var tempStr string
	var tempDiff uint32
	tempDiff = 0xffffffff

	for ID, value := range ls.virtualIDRing {
		//fmt.Println("inputID, ringID, diff", vID, ID, uint32(ID - vID))
		if tempDiff > uint32(ID-vID) {
			tempDiff = uint32(ID - vID)
			tempStr = value
		}
	}
	//fmt.Println("the best diff is", tempDiff)
	return tempStr
}
