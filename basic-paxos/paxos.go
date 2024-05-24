package basicpaxos

import (
  "sync"
  "net"
  "net/rpc"
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

// Proposal represents a proposal with a number and a value
// we set Value of type interface because of flexibility,
// for the assignment 1, you could set it as int
type Proposal struct {
  Number int
  Value interface{} 
}

// rpc args for prepare
type PrepareArgs struct {
  ProposalNumber int
}

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

func (pn *PaxosNode) acceptConn() {
  for {
    conn, err := pn.listener.Accept()
    if err != nil {
      panic(err)
    }
    go pn.server.ServeConn(conn)
  }
}












