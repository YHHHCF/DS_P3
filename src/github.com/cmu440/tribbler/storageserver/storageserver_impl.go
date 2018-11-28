package storageserver

import (
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type storageServer struct {
	hostPort string

	isMaster   bool              // whether the storage server is master server
	nodes      []storagerpc.Node // all the storage servers
	numNodes   int               // number of all the servers on the ring
	virtualIDs []uint32          // virtualIDs for this storage server

	listMap     map[string][]string                 // map for list storing client and tribbler ID
	itemMap     map[string]string                   // map for item storing tribblers
	revokingMap map[string]string                   // map for revoking lease request
	leaseMap    map[string]map[string]*leaseContent // map a key to leases (different hostports)

	sServerLock     *sync.Mutex
	listMapLock     *sync.Mutex
	itemMapLock     *sync.Mutex
	revokingMapLock *sync.Mutex
	revokingMapCond *sync.Cond
	leaseMapLock    *sync.Mutex
}

type leaseContent struct {
	expTime  time.Time
	hostPort string
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// virtualIDs is a list of random, unsigned 32-bits IDs identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, virtualIDs []uint32) (StorageServer, error) {
	sServer := &storageServer{
		nodes:           []storagerpc.Node{},
		virtualIDs:      virtualIDs,
		numNodes:        numNodes,
		itemMap:         make(map[string]string),
		listMap:         make(map[string][]string),
		revokingMap:     make(map[string]string),
		leaseMap:        make(map[string]map[string]*leaseContent),
		hostPort:        fmt.Sprintf("localhost:%d", port),
		sServerLock:     new(sync.Mutex),
		itemMapLock:     new(sync.Mutex),
		listMapLock:     new(sync.Mutex),
		revokingMapLock: new(sync.Mutex),
		leaseMapLock:    new(sync.Mutex)}
	sServer.revokingMapCond = sync.NewCond(sServer.revokingMapLock)

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	// Wrap the storageServer before registering it for RPC.
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(sServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	registerReply := new(storagerpc.RegisterReply)
	registerArgs := new(storagerpc.RegisterArgs)
	registerArgs.ServerInfo = storagerpc.Node{sServer.hostPort, virtualIDs}
	// if this is master server, add it into servers
	if masterServerHostPort == "" {
		sServer.isMaster = true
		sServer.nodes = append(sServer.nodes, registerArgs.ServerInfo)
	} else { // if this is slave server, call registerServer through rpc
		sServer.isMaster = false
		client, err := rpc.DialHTTP("tcp", masterServerHostPort)
		// Keep trying to connect with masterServer
		for {
			if err == nil {
				break
			}
			time.Sleep(time.Duration(1000 * time.Millisecond))
			client, err = rpc.DialHTTP("tcp", masterServerHostPort)
		}

		// Wait until all the storagesevers are registered
		client.Call("StorageServer.RegisterServer", registerArgs, registerReply)
		for {
			if registerReply.Status == storagerpc.OK {
				break
			}
			time.Sleep(time.Duration(1000 * time.Millisecond))
			client.Call("StorageServer.RegisterServer", registerArgs, registerReply)
		}
		sServer.nodes = registerReply.Servers
	}
	return sServer, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.sServerLock.Lock()
	defer ss.sServerLock.Unlock()

	// check whether the server exist in the server list
	existFlag := 0
	for i := 0; i < len(ss.nodes); i++ {
		if ss.nodes[i].HostPort == args.ServerInfo.HostPort {
			existFlag = 1
			break
		}
	}

	// server did not exist before, add it into server list
	if existFlag == 0 {
		ss.nodes = append(ss.nodes, args.ServerInfo)
	}

	// check whether it is ready to start serving（all the slave servers are registered）
	if len(ss.nodes) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodes
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.sServerLock.Lock()
	// check whether all the servers are registered
	if len(ss.nodes) == ss.numNodes {
		reply.Servers = ss.nodes
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady

	}
	ss.sServerLock.Unlock()
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	// check whether item is in this server
	if ss.CheckNode(args.Key) {
		ss.itemMapLock.Lock()
		// check whether the key is in the node
		if value, hasKey := ss.itemMap[args.Key]; hasKey {
			reply.Value = value
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.KeyNotFound
		}
		ss.itemMapLock.Unlock()

		//if key does not exist in the server, still grant a lease
		if args.WantLease {
			reply.Lease = *ss.GrantLease(args.Key, args.HostPort)
		}
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	// check whether item is in this server
	if ss.CheckNode(args.Key) {
		// Store the key which need to be revoked
		// in case of there are multiple revoke request for the same key
		ss.InsertRevoking(args.Key)
		// as long as leases are ready to be revoked
		ss.RevokeLease(args.Key)
		// after fininshing revoking leases, delete the key in revokingMap
		defer ss.DeleteRevoking(args.Key)

		ss.itemMapLock.Lock()
		defer ss.itemMapLock.Unlock()

		// delete item if itemMap has key
		if _, haskey := ss.itemMap[args.Key]; haskey {
			delete(ss.itemMap, args.Key)
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.KeyNotFound
		}
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if ss.CheckNode(args.Key) {
		ss.listMapLock.Lock()
		if list, haskey := ss.listMap[args.Key]; haskey {
			reply.Status = storagerpc.OK
			reply.Value = make([]string, len(list))
			copy(reply.Value, list)
		} else {
			reply.Status = storagerpc.KeyNotFound
		}
		ss.listMapLock.Unlock()

		if args.WantLease {
			reply.Lease = *ss.GrantLease(args.Key, args.HostPort)
		}
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if ss.CheckNode(args.Key) {
		// Store the key which need to be revoked
		// in case of there are multiple revoke request for the same key
		ss.InsertRevoking(args.Key)
		// as long as leases are ready to be revoked
		ss.RevokeLease(args.Key)

		defer ss.DeleteRevoking(args.Key)

		ss.itemMapLock.Lock()
		defer ss.itemMapLock.Unlock()

		// insert tribbler into itemMap
		ss.itemMap[args.Key] = args.Value
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if ss.CheckNode(args.Key) {
		// Store the key which need to be revoked
		// in case of there are multiple revoke request for the same key
		ss.InsertRevoking(args.Key)
		// as long as leases are ready to be revoked
		ss.RevokeLease(args.Key)
		// after fininshing revoking leases, delete the key in revokingMap
		defer ss.DeleteRevoking(args.Key)

		ss.listMapLock.Lock()
		defer ss.listMapLock.Unlock()

		list, hasKey := ss.listMap[args.Key]
		if hasKey {
			len := len(list)
			i := 0
			for i = 0; i < len; i++ {
				if args.Value == list[i] {
					break
				}
			}
			if i <= len-1 {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}

		ss.listMap[args.Key] = append(ss.listMap[args.Key], args.Value)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if ss.CheckNode(args.Key) {
		// Store the key which need to be revoked
		// in case of there are multiple revoke request for the same key
		ss.InsertRevoking(args.Key)
		// as long as leases are ready to be revoked
		ss.RevokeLease(args.Key)
		// after fininshing revoking leases, delete the key in revokingMap
		defer ss.DeleteRevoking(args.Key)

		ss.listMapLock.Lock()
		defer ss.listMapLock.Unlock()

		list, hasKey := ss.listMap[args.Key]
		if !hasKey {
			reply.Status = storagerpc.KeyNotFound
			return nil
		}

		// delete value with key in the list
		len := len(list)

		i := 0
		for i = 0; i < len; i++ {
			if args.Value == list[i] {
				break
			}
		}

		if len > 0 {
			// when value with key is at the end of list
			if i == len-1 {
				ss.listMap[args.Key] = list[:i]
			} else if i < len-1 {
				//  when value with key is at the head of list
				if i == 0 {
					ss.listMap[args.Key] = list[i+1:]
				} else { // when value with key is in the middle of list
					ss.listMap[args.Key] = append(list[:i-1], list[i+1:]...)
				}
			} else {
				reply.Status = storagerpc.ItemNotFound
				return nil
			}
			reply.Status = storagerpc.OK
			return nil
		} else {
			reply.Status = storagerpc.ItemNotFound
			return nil
		}
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) GrantLease(key string, hostport string) *storagerpc.Lease {
	ss.revokingMapLock.Lock()
	result := new(storagerpc.Lease)
	// lease with the same key is being revoked, cannot grant the lease
	if _, haskey := ss.revokingMap[key]; haskey {
		ss.revokingMapLock.Unlock()
		result.Granted = false
		result.ValidSeconds = 0
	} else {
		ss.revokingMapLock.Unlock()
		ss.leaseMapLock.Lock()
		lease := &leaseContent{
			hostPort: hostport,
			expTime:  time.Now().Add(time.Duration((storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second))}
		// first lease request for key, create leaseMap item
		if leases, haskey := ss.leaseMap[key]; !haskey {
			ss.leaseMap[key] = make(map[string]*leaseContent)
			ss.leaseMap[key][hostport] = lease
		} else { // otherwise, update the lease
			leases[hostport] = lease
		}
		ss.leaseMapLock.Unlock()
		result.Granted = true
		result.ValidSeconds = storagerpc.LeaseSeconds
	}
	return result
}

func (ss *storageServer) InsertRevoking(key string) {
	ss.revokingMapLock.Lock()
	_, haskey := ss.revokingMap[key]
	// a revoking request is being delt with at the same time
	// wait until the recent revoking request finish
	for haskey == true {
		ss.revokingMapCond.Wait()
		_, haskey = ss.revokingMap[key]
	}
	// insert revoking request with key into revokingMap
	ss.revokingMap[key] = ""
	ss.revokingMapLock.Unlock()
}

func (ss *storageServer) DeleteRevoking(key string) {
	ss.revokingMapLock.Lock()
	defer ss.revokingMapLock.Unlock()
	// finish this revoking action
	delete(ss.revokingMap, key)
	// notify the revoking request with the same key
	ss.revokingMapCond.Broadcast()
}

// only revoke lease when there is modification to the storage
func (ss *storageServer) RevokeLease(key string) {
	// check whether the leases with key are able to be revoked
	ss.leaseMapLock.Lock()
	leases, haskey := ss.leaseMap[key]
	ss.leaseMapLock.Unlock()
	if haskey {
		// Revoke leases with key
		wg := new(sync.WaitGroup)
		for hostport, lease := range leases {
			if lease.expTime.After(time.Now()) {
				wg.Add(1)
				go ss.NotifyLibRevokeLease(key, lease, hostport, wg)
			}
		}
		// wait until all the leases are revoked from libServer
		wg.Wait()
		delete(ss.leaseMap, key)
	} else {
		return
	}

}

func (ss *storageServer) NotifyLibRevokeLease(key string, lease *leaseContent, hostport string, wg *sync.WaitGroup) {
	success := 0
	for {
		libClient, err := rpc.DialHTTP("tcp", hostport)
		if err != nil {
			continue
		}
		revokeArgs := new(storagerpc.RevokeLeaseArgs)
		revokeReply := new(storagerpc.RevokeLeaseReply)
		revokeArgs.Key = key
		revokeLease := libClient.Go("LeaseCallbacks.RevokeLease", revokeArgs, revokeReply, nil)
		select {
		// libServer finish revoking lease
		case <-revokeLease.Done:
			if revokeReply.Status == storagerpc.OK {
				success = 1
			}
		// lease expired
		case <-time.After(lease.expTime.Sub(time.Now())):
			success = 1
		}
		if success == 1 {
			wg.Done()
			return
		}
	}
}

// get the hostPort with the nearest vID which is equal or greater than the given ID
func (ss *storageServer) GetNearestNodeIndex(key string) uint32 {
	hash := libstore.StoreHash(key)
	next := ^uint32(0)
	min := ^uint32(0)
	for _, node := range ss.nodes {
		for _, id := range node.VirtualIDs {
			if id >= hash && id < next {
				next = id
			}
			if id < min {
				min = id
			}
		}
	}
	if next != ^uint32(0) {
		return next
	} else {
		return min
	}
}

// check whether the virtulID suitable for key is in this server
func (ss *storageServer) CheckNode(key string) bool {
	ID := ss.GetNearestNodeIndex(key)
	for _, value := range ss.virtualIDs {
		if ID == value {
			return true
		}
	}
	return false
}
