package storageserver

import (
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

type storageServer struct {
	//masterServerHostPort string
	hostPort string

	isMaster   bool
	nodes      []storagerpc.Node
	numNodes   int
	virtualIDs []uint32

	listMap     map[string][]string
	itemMap     map[string]string
	revokingMap map[string]string
	leaseMap    map[string]map[string]*leaseContent

	sServerLock     *sync.Mutex // protect the R/W of server's content
	listMapLock     *sync.Mutex
	itemMapLock     *sync.Mutex
	revokingMapLock *sync.Mutex
	revokingMapCond *sync.Cond
	leaseMapLock    *sync.Mutex

	client *rpc.Client // the connection to the master

}

type leaseContent struct {
	expTime  time.Time
	hostport string
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
	//fmt.Println(masterServerHostPort, numNodes, port, virtualIDs)
	sServer := new(storageServer)
	sServer.nodes = []storagerpc.Node{}
	sServer.virtualIDs = virtualIDs
	sServer.numNodes = numNodes
	sServer.itemMap = make(map[string]string)
	sServer.listMap = make(map[string][]string)
	sServer.revokingMap = make(map[string]string)
	sServer.leaseMap = make(map[string]map[string]*leaseContent)
	sServer.hostPort = fmt.Sprintf("localhost:%d", port)
	sServer.sServerLock = new(sync.Mutex)
	sServer.itemMapLock = new(sync.Mutex)
	sServer.listMapLock = new(sync.Mutex)
	sServer.revokingMapLock = new(sync.Mutex)
	sServer.revokingMapCond = sync.NewCond(sServer.revokingMapLock)
	sServer.leaseMapLock = new(sync.Mutex)

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
		for {
			if err == nil {
				break
			}
			time.Sleep(time.Duration(1000 * time.Millisecond))
			client, err = rpc.DialHTTP("tcp", masterServerHostPort)
		}

		sServer.client = client
		var registerStatus = storagerpc.NotReady
		for registerStatus != storagerpc.OK {
			time.Sleep(time.Second)
			client.Call("StorageServer.RegisterServer", registerArgs, registerReply)
			registerStatus = registerReply.Status
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
	defer ss.sServerLock.Unlock()
	// check whether all the servers are registered
	if len(ss.nodes) != ss.numNodes {
		reply.Status = storagerpc.NotReady
		return nil
	}

	reply.Servers = ss.nodes
	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	//index := ss.GetNearestNodeIndex(args.Key)
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
			reply.Lease = *ss.GiveLease(args.Key, args.HostPort)
		}
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	//index := ss.GetNearestNodeIndex(args.Key)
	if ss.CheckNode(args.Key) {
		ss.SetRevoking(args.Key)
		ss.RevokeLease(args.Key)
		defer ss.ClearRevoking(args.Key)

		ss.itemMapLock.Lock()
		defer ss.itemMapLock.Unlock()

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
	//index := ss.GetNearestNodeIndex(args.Key)
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
			reply.Lease = *ss.GiveLease(args.Key, args.HostPort)
		}
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//index := ss.GetNearestNodeIndex(args.Key)
	if ss.CheckNode(args.Key) {
		ss.SetRevoking(args.Key)
		ss.RevokeLease(args.Key)
		defer ss.ClearRevoking(args.Key)

		ss.itemMapLock.Lock()
		defer ss.itemMapLock.Unlock()

		ss.itemMap[args.Key] = args.Value
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if ss.CheckNode(args.Key) {
		ss.SetRevoking(args.Key)
		ss.RevokeLease(args.Key)
		defer ss.ClearRevoking(args.Key)

		ss.listMapLock.Lock()
		defer ss.listMapLock.Unlock()

		key := args.Key
		value := args.Value

		list, hasKey := ss.listMap[key]
		if hasKey {
			len := len(list)

			i := 0
			for i = 0; i < len; i++ {
				if value == list[i] {
					break
				}
			}

			if i <= len-1 {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}

		ss.listMap[key] = append(ss.listMap[key], value)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.WrongServer
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if ss.CheckNode(args.Key) {
		ss.SetRevoking(args.Key)
		ss.RevokeLease(args.Key)
		defer ss.ClearRevoking(args.Key)

		ss.listMapLock.Lock()
		defer ss.listMapLock.Unlock()

		key := args.Key
		value := args.Value

		list, hasKey := ss.listMap[key]
		if !hasKey {
			reply.Status = storagerpc.KeyNotFound
			return nil
		}

		len := len(list)

		i := 0
		for i = 0; i < len; i++ {
			if value == list[i] {
				break
			}
		}

		if len > 0 {
			if i == len-1 {
				ss.listMap[key] = list[:i]
			} else if i < len-1 {
				if i == 0 {
					ss.listMap[key] = list[i+1:]
				} else {
					ss.listMap[key] = append(list[:i-1], list[i+1:]...)
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

func (ss *storageServer) GiveLease(key string, hostport string) *storagerpc.Lease {
	ss.revokingMapLock.Lock()
	if _, haskey := ss.revokingMap[key]; haskey {
		ss.revokingMapLock.Unlock()
		return &storagerpc.Lease{
			Granted:      false,
			ValidSeconds: 0,
		}
	} else {
		ss.revokingMapLock.Unlock()
		ss.leaseMapLock.Lock()
		lease := new(leaseContent)
		lease.hostport = hostport
		lease.expTime = time.Now().Add(time.Duration((storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second))

		if v, haskey := ss.leaseMap[key]; !haskey {
			ss.leaseMap[key] = make(map[string]*leaseContent)
			ss.leaseMap[key][hostport] = lease
		} else {
			v[hostport] = lease
		}
		ss.leaseMapLock.Unlock()
		return &storagerpc.Lease{
			Granted:      true,
			ValidSeconds: storagerpc.LeaseSeconds,
		}
	}
}

func (ss *storageServer) IsRevoking(key string) bool {
	ss.revokingMapLock.Lock()
	defer ss.revokingMapLock.Unlock()
	if _, haskey := ss.revokingMap[key]; haskey {
		return true
	} else {
		return false
	}
}

func (ss *storageServer) SetRevoking(key string) {
	ss.revokingMapLock.Lock()
	_, haskey := ss.revokingMap[key]
	for haskey == true {
		ss.revokingMapCond.Wait()
		_, haskey = ss.revokingMap[key]
	}

	ss.revokingMap[key] = ""
	ss.revokingMapLock.Unlock()
}

func (ss *storageServer) ClearRevoking(key string) {

	ss.revokingMapLock.Lock()
	defer ss.revokingMapLock.Unlock()

	delete(ss.revokingMap, key)
	ss.revokingMapCond.Broadcast()
}

//only revoke lease when write
func (ss *storageServer) RevokeLease(key string) {
	ss.leaseMapLock.Lock()
	leases, haskey := ss.leaseMap[key]
	ss.leaseMapLock.Unlock()
	if !haskey {
		return
	}

	wg := new(sync.WaitGroup)

	for hostport, lease := range leases {
		if lease.expTime.After(time.Now()) {
			wg.Add(1)
			go func() {
				status := "no"
				for status == "no" {

					libClient, err := rpc.DialHTTP("tcp", hostport)

					if err != nil {
						continue
					}

					revokeArgs := new(storagerpc.RevokeLeaseArgs)
					revokeArgs.Key = key
					revokeReply := new(storagerpc.RevokeLeaseReply)
					revokeLease := libClient.Go("LeaseCallbacks.RevokeLease", revokeArgs, revokeReply, nil)
					select {
					case <-revokeLease.Done:
						if revokeReply.Status == storagerpc.OK {
							status = "ok"
						}
					case <-time.After(lease.expTime.Sub(time.Now())):
						status = "ok"
					}

				}
				wg.Done()
			}()
			//go ss.RevokeLeaseSubRoutine(key, hostport, lease)

		}
	}
	wg.Wait()
	delete(ss.leaseMap, key)
}

//func (ss *storageServer) RevokeLeaseSubRoutine(key string, hostport string, lease *leaseContent) {
//	status := "no"
//	for status == "no" {
//
//		libClient, err := rpc.DialHTTP("tcp", hostport)
//
//		if err != nil {
//			continue
//		}
//
//		revokeArgs := new(storagerpc.RevokeLeaseArgs)
//		revokeArgs.Key = key
//		revokeReply := new(storagerpc.RevokeLeaseReply)
//		revokeLease := libClient.Go("LeaseCallbacks.RevokeLease", revokeArgs, revokeReply, nil)
//		select {
//		case <-revokeLease.Done:
//			if revokeReply.Status == storagerpc.OK {
//				status = "ok"
//			}
//		case <-time.After(lease.expTime.Sub(time.Now())):
//			status = "ok"
//		}
//
//	}
//	wg.Done()
//}

func (ss *storageServer) GetNearestNodeIndex(key string) uint32 {
	//args := &storagerpc.GetServersArgs{}
	//var reply storagerpc.GetServersReply

	//ss.client.Call("StorageServer.GetServers", args, &reply)

	hash := libstore.StoreHash(key)
	var next = ^uint32(0)
	var nextIndex = -1
	var min = ^uint32(0)
	//var minIndex = -1
	for i, node := range ss.nodes {
		for _, id := range node.VirtualIDs {
			if id >= hash && id < next {
				next = id
				nextIndex = i
			}
			if id < min {
				min = id
			}
		}
	}

	if nextIndex != -1 {
		return next
	} else {
		return min
	}

}

func (ss *storageServer) CheckNode(key string) bool {
	ID := ss.GetNearestNodeIndex(key)
	for _, value := range ss.virtualIDs {
		if ID == value {
			return true
		}
	}
	return false
}
