package tribserver

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type tribServer struct {
	l libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	tServer := new(tribServer)
	var err error
	tServer.l, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, err
	}

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", fmt.Sprintf("%s", myHostPort))
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	return tServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userKey := util.FormatUserKey(args.UserID)

	user, _ := ts.l.Get(userKey)

	if user != "" {
		// userId exists
		reply.Status = tribrpc.Exists
		return nil
	} else { // userId does not exist
		putErr := ts.l.Put(userKey, args.UserID)
		reply.Status = tribrpc.OK
		return putErr
	}
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// confirm two users exit or not
	userKey := util.FormatUserKey(args.UserID)
	targetKey := util.FormatUserKey(args.TargetUserID)

	usr, _ := ts.l.Get(userKey)
	tUsr, _ := ts.l.Get(targetKey)

	// server should not allow a nonexistent user ID to subscribe to anyone
	if usr == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	// server should not allow a user to subscribe to a nonexistent user ID
	if tUsr == "" {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	userListKey := util.FormatSubListKey(args.UserID)
	appendErr := ts.l.AppendToList(userListKey, args.TargetUserID)

	if appendErr == nil {
		reply.Status = tribrpc.OK
	} else { // targetUser has already been subscribed
		// existing subscription do not count error here
		reply.Status = tribrpc.Exists
		appendErr = nil
	}
	return appendErr
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// confirm two users exit or not
	userKey := util.FormatUserKey(args.UserID)
	targetKey := util.FormatUserKey(args.TargetUserID)

	usr, _ := ts.l.Get(userKey)
	tUsr, _ := ts.l.Get(targetKey)

	// server should not allow a nonexistent user ID to subscribe to anyone
	if usr == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	// server should not allow a user to subscribe to a nonexistent user ID
	if tUsr == "" {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	userListKey := util.FormatSubListKey(args.UserID)
	removeErr := ts.l.RemoveFromList(userListKey, args.TargetUserID)

	if removeErr == nil {
		reply.Status = tribrpc.OK
	} else { // targetUser has already been removed or never exist
		reply.Status = tribrpc.NoSuchTargetUser
	}
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	userKey := util.FormatUserKey(args.UserID)
	// userId dose not exist
	usr, _ := ts.l.Get(userKey)
	if usr == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userListKey := util.FormatSubListKey(args.UserID)
	// user has subscribers
	if tmpStr, err := ts.l.GetList(userListKey); err == nil {
		// check each subsrciber also subscribe to the user
		for i := 0; i < len(tmpStr); i++ {
			targetListKey := util.FormatSubListKey(tmpStr[i])
			targetSubList, targetErr := ts.l.GetList(targetListKey)
			if targetErr != nil {
				continue
			} else {
				for j := 0; j < len(targetSubList); j++ {
					if targetSubList[j] == args.UserID {
						reply.UserIDs = append(reply.UserIDs, tmpStr[i])
						break
					}
				}
			}
		}

	} else {
		reply.UserIDs = make([]string, 0)
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	userKey := util.FormatUserKey(args.UserID)
	// userId dose not exist
	usr, _ := ts.l.Get(userKey)
	if usr == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userTribListKey := util.FormatTribListKey(args.UserID)
	postTime := time.Now()
	postKey := util.FormatPostKey(args.UserID, postTime.UnixNano())

	reply.PostKey = postKey
	// append postKey to user's list
	ts.l.AppendToList(userTribListKey, postKey)

	// wrap the tribbler and put it into storage
	tri := tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   postTime,
		Contents: args.Contents,
	}
	marshalTri, _ := json.Marshal(tri)
	ts.l.Put(postKey, string(marshalTri))
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	userKey := util.FormatUserKey(args.UserID)
	// userId dose not exist
	usr, _ := ts.l.Get(userKey)
	if usr == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	deleErr := ts.l.Delete(args.PostKey)
	if deleErr != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	userTribListKey := util.FormatTribListKey(args.UserID)
	removeErr := ts.l.RemoveFromList(userTribListKey, args.PostKey)
	if removeErr != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userKey := util.FormatUserKey(args.UserID)
	// userId dose not exist
	usr, _ := ts.l.Get(userKey)

	if usr == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userTribListKey := util.FormatTribListKey(args.UserID)
	tribs, getErr := ts.l.GetList(userTribListKey)

	if getErr == nil {
		reply.Tribbles = ts.SortByPostTime(tribs)
	} else {
		reply.Tribbles = make([]tribrpc.Tribble, 0)
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userKey := util.FormatUserKey(args.UserID)
	// userId dose not exist
	usr, _ := ts.l.Get(userKey)

	if usr == "" {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	userListKey := util.FormatSubListKey(args.UserID)
	subscribers, err := ts.l.GetList(userListKey)

	if err != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = nil
		return nil
	}

	tribs := make([]string, 0)
	for _, s := range subscribers {
		sTribListKey := util.FormatTribListKey(s)
		sTribList, err := ts.l.GetList(sTribListKey)
		if err == nil {
			tribs = append(tribs, sTribList...)
		}
	}

	reply.Tribbles = ts.SortByPostTime(tribs)
	reply.Status = tribrpc.OK
	return nil
}

type tribsSorted []tribrpc.Tribble

func (ts *tribServer) SortByPostTime(tribs []string) []tribrpc.Tribble {
	var result []tribrpc.Tribble

	for _, postKey := range tribs {
		marshalTri, err := ts.l.Get(postKey)
		if err == nil {
			unmarshalTri := tribrpc.Tribble{}
			json.Unmarshal([]byte(marshalTri), &unmarshalTri)
			result = append(result, unmarshalTri)
		}
	}

	result = tribsSorted(result)

	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Posted.UnixNano() > result[i].Posted.UnixNano() {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	if len(result) > 100 {
		result = result[0:100]
	}
	return result
}
