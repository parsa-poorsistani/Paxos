package basicpaxos

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// This struct is for persisting the crucial values on Disk
/*type PaxosNodeState struct {
    MaxSeen int64 json:"max_seen"
    AcceptedNum int64 json:"accepted_num"
    AcceptedValue interface{} json:"accepted_value"
}*/


// PaxosNode represents servers in the cluster, each node can handle three aganes mentioned in the paper
// Proposer, Acceptor and Learner
// You must add the necessary fields to this struct, don't remove default fields.
type PaxosNode struct {
  mu sync.Mutex
  id int
  maxSeen int64 //used in proposal phase
  acceptedNum int64 //not sure about this
  acceptedValue interface{}
  peers []string // other servers addr
  server *rpc.Server
  listener net.Listener
  proposalSeq int64 // monotonically increasing seq number
}

// Proposal represents a proposal with a number and a value.
// We set Value of type interface{} for flexibility, but for assignment 1, you could set it as int.
type Proposal struct {
  Number int64
  Value interface{} 
}

// rpc args for prepare
type PrepareArgs struct {
  ProposalNumber int64
}

// PrepareReply defines the RPC reply for the Prepare phase.
type PrepareReply struct {
  Promise bool
  AcceptedNum int64
  AcceptedValue interface{}
}

type AcceptArgs struct {
  Proposal Proposal
}

type AcceptReply struct {
  Accepted bool
}

type LearnArgs struct {
    //ProposalNumber int64
    //Value interface{}
}

type LearnReply struct {
  Value interface{}
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
    pn.mu.Lock()
    defer pn.mu.Unlock()

    if args.ProposalNumber > pn.maxSeen {
        pn.maxSeen = args.ProposalNumber
        reply.Promise = true
        reply.AcceptedNum = pn.acceptedNum
        reply.AcceptedValue = pn.acceptedValue
    } else {
        reply.Promise = false
    }

    return nil
}

func (pn *PaxosNode) Accept(args AcceptArgs, reply *AcceptReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    if args.Proposal.Number >= pn.maxSeen {
        pn.maxSeen = args.Proposal.Number
        pn.acceptedNum = args.Proposal.Number
        pn.acceptedValue = args.Proposal.Value
        reply.Accepted = true
    } else {
        reply.Accepted = false
    }
    return nil
}

func (pn *PaxosNode) Learn(args LearnArgs, reply *LearnReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    //pn.acceptedNum = args.ProposalNumber
    //pn.acceptedValue = args.Value
    reply.Value = pn.acceptedValue
    return nil
}

// Proposer
func (pn *PaxosNode) Propose(value interface{}) {
    prposalNum := pn.generateProposalNumber()
    proposal := Proposal{Number: prposalNum, Value: value}

    prepareArgs := PrepareArgs{ProposalNumber: proposal.Number}
    prepareReply := PrepareReply{}

    var maxAcceptedNumber int64 = -1
    var maxAcceptedValue interface{}
    promises := 0

    // making the RPC call to other nodes to collect vote
    for _,peer := range pn.peers {
        if Call(peer, "PaxosNode.Prepare", prepareArgs, &prepareReply) && prepareReply.Promise {
            promises++
            if prepareReply.AcceptedNum > maxAcceptedNumber {
                maxAcceptedNumber = prepareReply.AcceptedNum
                maxAcceptedValue = prepareReply.AcceptedValue
            }
        }
    }
    
    // checking for the majority
    if promises > len(pn.peers)/2 {

        // if any acceptor has accepted a value, propose that value
        if maxAcceptedValue != nil {
            proposal.Value = maxAcceptedValue
        }

        acceptedArgs := AcceptArgs{Proposal: proposal}
        acceptedReply := AcceptReply{}

        accepted := 0
        for _, peer := range pn.peers {
            if Call(peer, "PaxosNode.Accept", acceptedArgs, &acceptedReply) && acceptedReply.Accepted {
                accepted++
            }
        }
        
        if accepted > len(pn.peers)/2 {
            // value is chosen and now should change the node's acceptedValue
            pn.mu.Lock()
            pn.acceptedNum = proposal.Number
            pn.acceptedValue = proposal.Value
            pn.mu.Unlock()


            // Value is chosed and must inform learners (if necessary)
            //learnArgs := LearnArgs{}
            //learReply := LearnReply{}
            //for _, peer := range pn.peers {
            //    Call(peer, "PaxosNode.Lear", learnArgs, &learReply)
            //}
        }
    } 
} 

// Learner
func (pn *PaxosNode) LearnValue() interface{} {
    learnArgs := LearnArgs{}
    learnReply := LearnReply{}

    // query all peers
    for _, peer := range pn.peers {
        if Call(peer, "PaxosNode.Learn", learnArgs, &learnReply) {
            return learnReply.Value
        }
    }
    return nil
}


func(pn *PaxosNode) generateProposalNumber() int64 {
    pn.mu.Lock()
    defer pn.mu.Unlock()
    
    pn.proposalSeq++
    
    // combine seq number with node ID to generate a unqiue proposalNumber
    
    proposalNumber := (pn.proposalSeq << 16) | int64(pn.id)
    return proposalNumber
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
