// basic-Paxos is designed for single variable consensus
// for real world production level programs which needs to store multiple values
// we should use multi-Paxos

package basicpaxos

import (
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

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
    stateFile string // Path to the file for durable storage
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
func NewPaxosNode(id int, peers []string, address string, statePath string) *PaxosNode {
  node := &PaxosNode{
    id: id,
    peers: peers,
    stateFile: statePath,
  }
    
  node.loadState()
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

// loadState loads the acceptor state from a file.
func(pn *PaxosNode) loadState() {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    if _, err := os.Stat(pn.stateFile); os.IsNotExist(err) {
        return
    }

    data, err := os.ReadFile(pn.stateFile)
    if err != nil {
        fmt.Print("ReadFile at loadState()\n")
        panic(err)
    }
    
    state := make(map[string]interface{})
    if err := json.Unmarshal(data, &state); err != nil {
        fmt.Print("panic at unmarshal\n")
        panic(err)
    }

    if val, ok := state["maxSeen"].(float64); ok {
        pn.maxSeen = int64(val)
    }
    if val, ok := state["acceptedNum"].(float64); ok {
        pn.acceptedNum = int64(val)
    }
    if val, ok := state["acceptedValue"]; ok {
        pn.acceptedValue = val
    }
}

// saveState saves the acceptor state to a file.
func (pn *PaxosNode) saveState() {

    pn.mu.Lock()
    defer pn.mu.Unlock()

    state := map[string]interface{}{
        "maxSeen":       pn.maxSeen,
        "acceptedNum":   pn.acceptedNum,
        "acceptedValue": pn.acceptedValue,
    }

    data, err := json.Marshal(state)
    if err != nil {
        panic(err)
    }

    if err := os.WriteFile(pn.stateFile, data, 0644); err != nil {
        panic(err)
    }
}


// The Prepare RPC that should be implemented, don't change the signatures 
func (pn *PaxosNode) Prepare(args PrepareArgs, reply *PrepareReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    fmt.Printf("Node %d: args.pNumber %d and  pn.maxSeen %d\n", pn.id,args.ProposalNumber,pn.maxSeen)
    if args.ProposalNumber > pn.maxSeen {
        pn.maxSeen = args.ProposalNumber
        reply.Promise = true
        reply.AcceptedNum = pn.acceptedNum
        reply.AcceptedValue = pn.acceptedValue
        go pn.saveState()
        fmt.Printf("Node %d: Prepared proposal %d and Value: %v\n", pn.id, args.ProposalNumber, reply.AcceptedValue)
    } else {
        reply.Promise = false
        fmt.Printf("Node %d: Rejected proposal %d\n", pn.id, args.ProposalNumber)
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
        go pn.saveState()
        fmt.Printf("Node %d: Accepted proposal %d with value %v\n", pn.id, args.Proposal.Number, args.Proposal.Value)
    } else {
        reply.Accepted = false
        fmt.Printf("Node %d: Rejected proposal %d\n", pn.id, args.Proposal.Number)
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
    // shoule be 1 or 0? 1 makes more sense
    promises := 1

    // making the RPC call to other nodes to collect vote
    for _,peer := range pn.peers {
        // for testing purpose
        fmt.Printf("Node %d: Sending Prepare to peer %s\n", pn.id, peer)
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

        fmt.Printf("Node %d got majority %d of promises for pNum: %d and Value: %v\n",
            pn.id,promises,proposal.Number,proposal.Value)

        acceptedArgs := AcceptArgs{Proposal: proposal}
        acceptedReply := AcceptReply{}

        accepted := 1
        for _, peer := range pn.peers {
            fmt.Printf("Node %d: Sending Accept to peer %s\n", pn.id, peer)
            if Call(peer, "PaxosNode.Accept", acceptedArgs, &acceptedReply) && acceptedReply.Accepted {
                accepted++
            }
        }
        
        if accepted > len(pn.peers)/2 {
            fmt.Printf("Node %d got majority %d of accepted for pNum: %d and Value: %v\n",
                pn.id,accepted,proposal.Number,proposal.Value)

            // value is chosen and now should change the node's acceptedValue
            pn.mu.Lock()
            pn.acceptedNum = proposal.Number
            pn.acceptedValue = proposal.Value
            go pn.saveState()
            pn.mu.Unlock()
            fmt.Printf("Node %d: Proposal %d with value %v chosen\n", pn.id, proposal.Number, proposal.Value)
            // Value is chosed and must inform learners (if necessary)
            //learnArgs := LearnArgs{}
            //learReply := LearnReply{}
            //for _, peer := range pn.peers {
            //    Call(peer, "PaxosNode.Lear", learnArgs, &learReply)
            //}
        } else {
            fmt.Printf("Node %d: Proposal %d with value %v not accepted by majority\n", pn.id, proposal.Number, proposal.Value)
        }
    } else {
        fmt.Printf("Node %d: Proposal %d with value %v not promised by majority\n", pn.id, proposal.Number, proposal.Value)
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
//    fmt.Errorf("error in : %e", err)
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
        fmt.Println("timeout")
      return false
  }
}
