package basicpaxos

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// PaxosNode represents servers in the cluster, each node can handle three aganes mentioned in the paper
// Proposer, Acceptor and Learner
// You must add the necessary fields to this struct, don't remove default fields.
type PaxosNode struct {
  id int
  peers []string
  server *rpc.Server
  listener net.Listener
}
// Proposal represents a proposal with a number and a value.
// We set Value of type interface{} for flexibility, but for assignment 1, you could set it as int.
type Proposal struct {
  Number int
  Value interface{} 
}

// rpc args for prepare
type PrepareArgs struct {
  ProposalNumber int
}

// PrepareReply defines the RPC reply for the Prepare phase.
type PrepareReply struct {
}

type AcceptArgs struct {

}

type AcceptReply struct {

}

type LearnArgs struct {

}

type LearnReply struct {

}

// NewPaxosNode creates and initializes a new PaxosNode.
func NewPaxosNode(id int, peers []string, address string) *PaxosNode {
  node := &PaxosNode{
    id: id,
    peers: peers,
  }

  // the RPC server allows the node's methods to be called remotely.
  node.server = rpc.NewServer()
  node.server.Register(node)
  // this sets up a network listener which allows the Servers(Nodes) to commiunicate over a tcp network
  listener, err := net.Listen("tcp", address)
  if err != nil {
    panic(err)
  }
  node.listener = listener
  go node.acceptConn()
  return node
}

// acceptConn listens for incoming connections and serves them.
func (pn *PaxosNode) acceptConn() {
  for {
    conn, err := pn.listener.Accept()
    if err != nil {
      //panic(err) TODO: error handling
      continue
    }
    go pn.server.ServeConn(conn)
  }
}


// The Prepare RPC that should be implemented, don't change the signatures 
func (pn *PaxosNode) Prepare(args PrepareArgs, reply *PrepareReply) error {
  
}

func (pn *PaxosNode) Accept(args AcceptArgs, reply *AcceptReply) error {

}

func (pn *PaxosNode) Learn(args LearnArgs, reply *LearnReply) error {

}

// Proposer
func (pn *PaxosNode) Propose(value interface{}) {

} 

// Learner
func (pn *PaxosNode) LearnValue() interface{} {

  return nil
}

// Call simulates an RPC call to another Paxos node.
// addr is the address of the remote node, rpcName is the name of the RPC method to call,
// args are the arguments to pass to the RPC method 
//and reply is where the reply from the RPC method will be stored.
func Call(addr string, rpcName string, args interface{}, reply interface{}) bool {
  client, err := rpc.Dial("tcp", addr)
  if err != nil {
    fmt.Errorf("error: %e", err)
    return false
  }

  defer client.Close()

  // an async RPC call to avoid blocking
  // the nil parameter causes an internal channel creation
  call := client.Go(rpcName, args, reply, nil)
  select {
    case <- call.Done:
      return call.Error == nil 
    case <- time.After(1 * time.Second):
      return false
  }
}










