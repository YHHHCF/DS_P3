package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"strconv"
)

type libstore struct {
	masterServerHostPort string
	myHostPort           string
	leaseMode            LeaseMode

	client *rpc.Client
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	lServer := new(libstore)
	lServer.masterServerHostPort = masterServerHostPort
	lServer.myHostPort = myHostPort
	lServer.leaseMode = mode

	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	lServer.client = cli

	return lServer, nil
}

// if key does not exist, return non-nil error
func (ls *libstore) Get(key string) (string, error) {
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	var reply storagerpc.GetReply

	err := ls.client.Call("StorageServer.Get", args, &reply)

	if reply.Status != storagerpc.OK {
		err = errors.New("not ok")
	}

	return reply.Value, err
}

// if inserting key and value succeed, return nil error
func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply

	err := ls.client.Call("StorageServer.Put", args, &reply)

	if reply.Status != storagerpc.OK {
		err = errors.New("not ok")
	}

	return err
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply

	err := ls.client.Call("StorageServer.Delete", args, &reply)

	if reply.Status != storagerpc.OK {
		err = errors.New("not ok")
	}

	return err
}

// if key does not exist, return non-nil error
func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	var reply storagerpc.GetListReply

	err := ls.client.Call("StorageServer.GetList", args, &reply)

	if reply.Status != storagerpc.OK {
		err = errors.New("not ok")
	}

	return reply.Value, err
}

// if removeItem dose not exist, return non-nil error
func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply

	err := ls.client.Call("StorageServer.RemoveFromList", args, &reply)

	// if the item could not be found, return an error
	if reply.Status == storagerpc.ItemNotFound {
		return errors.New(strconv.Itoa(int(reply.Status)))
	}

	return err
}

// if newItem exists in the list, return non-nil error
func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply

	err := ls.client.Call("StorageServer.AppendToList", args, &reply)

	if reply.Status != storagerpc.OK {
		return errors.New(strconv.Itoa(int(reply.Status)))
	}

	return err
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
