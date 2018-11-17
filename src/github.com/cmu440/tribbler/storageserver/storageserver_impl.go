package storageserver

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	mux sync.Mutex

	masterServerHostPort string

	listMap map[string][]string
	itemMap map[string]string
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

	sServer := new(storageServer)

	sServer.masterServerHostPort = masterServerHostPort
	sServer.itemMap = make(map[string]string)
	sServer.listMap = make(map[string][]string)

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
	return sServer, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	//args.ServerInfo
	//reply.Servers
	//reply.Status
	return errors.New("not implemented RegisterServer")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	reply.Status = storagerpc.OK

	masterServer := storagerpc.Node{HostPort: ss.masterServerHostPort}
	var nodes []storagerpc.Node
	nodes = append(nodes, masterServer)

	reply.Servers = nodes
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	key := args.Key

	value, hasKey := ss.itemMap[key]

	if hasKey {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	reply.Value = value
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	key := args.Key
	delete(ss.itemMap, key)
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	key := args.Key

	reply.Status = storagerpc.OK
	reply.Value = ss.listMap[key]

	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	value := args.Value

	ss.itemMap[key] = value
	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	value := args.Value

	list, hasKey := ss.listMap[key]
	if hasKey {
		fmt.Println("AppendToList, has key")
		len := len(list)

		i := 0
		for i = 0; i < len; i++ {
			if value == list[i] {
				break
			}
		}

		if i <= len-1 {
			fmt.Println("exists")
			reply.Status = storagerpc.ItemExists
			return nil
		}
	}

	ss.listMap[key] = append(ss.listMap[key], value)

	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
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
			ss.listMap[key] = list[:i-1]
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

}
