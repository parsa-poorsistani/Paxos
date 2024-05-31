package multipaxos

import (
    "math/rand"
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// This struct represents the state of a single paxos.
// Each Paxos node maintains a map of PaxosInstance
// It's the log of entry commands
type PaxosInstance struct {
    maxSeen int64
    acceptedNum int64
    acceptedValue interface{}
    isAccepted bool
}

type PaxosNode struct {
    mu sync.Mutex
    id int
    instances map[int]*PaxosInstance
    proposalSeq int64
    peers []string
    server  *rpc.Server
    listener  net.Listener
    currentTerm int64
    isLeader bool
    leaderID  int
    statePath string
    heartbeatChan chan bool
    electionTimeout time.Duration
    hearbeatInterval  time.Duration
}

type Proposal struct {
    Number int64
    Value interface{}
}

type PrepareArgs struct {
    InstanceIdx int
    ProposalNumber int64
}

type PrepareReply struct {
    Promise       bool
    AcceptedNum   int64
    AcceptedValue interface{}
}

// RPC arguments and replies for Accept phase.
type AcceptArgs struct {
    Instance  int
    Proposal Proposal
}

type AcceptReply struct {
    Accepted bool
}

// RPC arguments and replies for Learn phase.
type LearnArgs struct {
    Instance int
}

type LearnReply struct {
    Value interface{}
}

type AddCommandArgs struct {
    Value interface{}
}

type AddCommandReply struct {
    CommandNum int
}

// RPC arguments and replies for listing commands.
type ListCommandsArgs struct{}

type ListCommandsReply struct {
    Commands map[int]interface{}
}

type RequestVoteArgs struct {
    Term        int64
    CandidateID int
}

type RequestVoteReply struct {
    VoteGranted bool
}

// RPC arguments and replies for Heartbeat phase.
type HeartbeatArgs struct {
    Term     int64
    LeaderID int
}

type HeartbeatReply struct{}


func Make(id int, peers []string, addr string, statePath string) *PaxosNode {
    node := &PaxosNode{
        id: id,
        peers: peers,
        instances: make(map[int]*PaxosInstance),
        statePath: statePath,
        isLeader: false,
        currentTerm: 0,
        leaderID: -1,
        heartbeatChan: make(chan bool),
        hearbeatInterval: time.Duration(500 * time.Millisecond),
        electionTimeout: time.Duration(1000 + rand.Intn(1501)) * time.Millisecond,
    }
    
    node.loadState()
    node.server = rpc.NewServer()
    node.server.Register(node)

    listener, err := net.Listen("tcp", addr)

    if err != nil {
        panic(err)
    }
    node.listener = listener
    go node.acceptConn()
    go node.startElectionTimeout()

    return node
}


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

    if _, err := os.Stat(pn.statePath); os.IsNotExist(err) {
        return
    }

    data, err := os.ReadFile(pn.statePath)
    if err != nil {
        panic(err)
    }
    
    state := make(map[int]*PaxosInstance)
    if err := json.Unmarshal(data, &state); err != nil {
        fmt.Print("panic at unmarshal\n")
        panic(err)
    }

    pn.instances = state
}

// saveState saves the acceptor state to a file.
func (pn *PaxosNode) saveState() {

    pn.mu.Lock()
    defer pn.mu.Unlock()

    state := pn.instances 

    data, err := json.Marshal(state)
    if err != nil {
        panic(err)
    }

    if err := os.WriteFile(pn.statePath, data, 0644); err != nil {
        panic(err)
    }
}


func(pn *PaxosNode) startElectionTimeout() {
    for {
        select {
            case <- pn.heartbeatChan:
                fmt.Printf("Node %d: received Heartbeat, resseting election timeout\n",pn.id)
            // not sure about this electionTimeoout
            case <- time.After(pn.electionTimeout):
                //election timeout, starting a new election
                fmt.Printf("Node %d: Election timeout, starting new election\n", pn.id)
                pn.startElection()
            // quiteChan?
        }
    }
}

func(pn *PaxosNode) startElection() {
    pn.mu.Lock()
    pn.currentTerm++
    term := pn.currentTerm
    pn.mu.Unlock()
    
    votes := 1

    var voteMu sync.Mutex
    var voteWg sync.WaitGroup
    for _, peer := range pn.peers {
        voteWg.Add(1)
        go func(peer string) {
            defer voteWg.Done()
            args := RequestVoteArgs{
                Term: term,
                CandidateID: pn.id,
            }
            var reply RequestVoteReply
            if Call(peer, "PaxosNode.RequestVote", args, &reply) && reply.VoteGranted {
                voteMu.Lock()
                votes++
                voteMu.Unlock()
            }

        }(peer)
    } 
    voteWg.Wait()

    if votes > len(pn.peers)/2 {
        pn.mu.Lock()
        pn.isLeader = true
        pn.leaderID = pn.id
        pn.mu.Unlock()
        go pn.sendHearbeats()
        go pn.performInitialPrepare()
    }
}



// Not sure about this whole thing
func(pn *PaxosNode) performInitialPrepare() {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    var wg sync.WaitGroup

    for i:=0;i<len(pn.instances); i++ {
        if instance, exists := pn.instances[i]; exists && instance.isAccepted {
            continue
        }
        wg.Add(1)
        go func(idx int) {
            defer wg.Done()
            
            // Perform the Prepare phase for this instance

            proposalNumber := pn.generateProposalNumber()
            prepareArgs := PrepareArgs{
                InstanceIdx: idx,
                ProposalNumber: proposalNumber,
            }
            var prepareReply PrepareReply

            promises := 1

            var maxAcceptedNum int64 = -1
            var maxAcceptedValue interface{}
            var peerWg sync.WaitGroup

            for _, peer := range pn.peers {
                wg.Add(1)
                go func(peer string) {
                    defer peerWg.Done()
                    if Call(peer, "PaxosNode.Prepare", prepareArgs, &prepareReply) && prepareReply.Promise {
                        pn.mu.Lock()
                        promises ++
                        if prepareReply.AcceptedNum > maxAcceptedNum {
                            maxAcceptedNum = prepareReply.AcceptedNum
                            maxAcceptedValue = prepareReply.AcceptedValue
                        }
                        pn.mu.Unlock()
                    }
                }(peer)
            }
            peerWg.Wait()

            if promises > len(pn.peers)/2 {
                if maxAcceptedValue != nil {
                    proposal := Proposal{
                        Number: proposalNumber,
                        Value: maxAcceptedValue,
                    }
                    acceptArgs := AcceptArgs{
                        Proposal: proposal,
                        Instance: idx, 
                    }
                    var acceptReply AcceptReply
                    var peerWg sync.WaitGroup
                    accepts := 1
                    for _, peer := range pn.peers {
                        wg.Add(1)
                        go func(peer string) {
                            defer peerWg.Done()
                            if Call(peer, "PaxosNode.Accept", acceptArgs, &acceptReply) && acceptReply.Accepted {
                                pn.mu.Lock()
                                accepts++
                                pn.mu.Unlock()
                            }
                        }(peer)
                    }
                    peerWg.Wait()

                    if accepts > len(pn.peers)/2 {
                        pn.mu.Lock()
                        instance := &PaxosInstance {
                            maxSeen: proposalNumber,
                            acceptedNum: proposalNumber,
                            acceptedValue: maxAcceptedValue,
                            isAccepted: true,
                        }
                        pn.instances[idx] = instance
                        pn.mu.Unlock()
                        go pn.saveState()
                    }
                }
            }
        }(i)

        wg.Wait()
    }
}
func(pn *PaxosNode) AddCommand(args AddCommandArgs, reply *AddCommandReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()
    if !pn.isLeader {
        // redirect to leader
        fmt.Printf("Node %d, is not leader\n, redirecting to %d", pn.id,pn.leaderID)
    }
    
    for i := 0; i<len(pn.instances); i++ {
        if _, exists := pn.instances[i]; !exists {
            
        }
    }
    return nil
} 
func(pn *PaxosNode) sendHearbeats() {
    for {
        pn.mu.Lock()
        if !pn.isLeader {
            return
        }
        currentTerm := pn.currentTerm
        pn.mu.Unlock()

        args := HeartbeatArgs{Term: currentTerm, LeaderID: pn.leaderID}
        reply := &HeartbeatReply{}
        var wg sync.WaitGroup
        for _, peer := range pn.peers {
            wg.Add(1)
            go func(peer string) {
                defer wg.Done()
                Call(peer, "PaxosNode.Heartbeat", args, reply)
            }(peer)
        }
        wg.Wait()

        time.Sleep(pn.hearbeatInterval)
    }
}


func(pn *PaxosNode) generateProposalNumber() int64 {
    pn.mu.Lock()
    defer pn.mu.Unlock()
    
    pn.proposalSeq++
    
    // combine seq number with node ID to generate a unqiue proposalNumber
    
    proposalNumber := (pn.proposalSeq << 16) | int64(pn.id)
    return proposalNumber
}

func (pn *PaxosNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    if args.Term > pn.currentTerm {
        pn.isLeader = false
        pn.currentTerm = args.Term
        reply.VoteGranted = true
    } else {
        reply.VoteGranted = false
    }

    return nil
}


func (pn *PaxosNode) Accept(args AcceptArgs, reply *AcceptReply) error {
    return nil
}

func (pn *PaxosNode) Hearbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    if args.Term >= pn.currentTerm {
        pn.leaderID = args.LeaderID
        pn.currentTerm = args.Term
        pn.isLeader = false
        pn.heartbeatChan <- true
    }
    return nil
}


func Call(addr, rpcMethod string, args interface{}, reply interface{}) bool {
    client, err := rpc.Dial("tcp",addr)
    if err != nil {
        fmt.Printf("Error in Call method: %e", err)
        return false
    }

    defer client.Close()
    call := client.Go(rpcMethod, args, reply, nil)
    
    select {
        case <- call.Done:
            return call.Error == nil
        case <- time.After(1 * time.Second):
            fmt.Printf("timeout for RPC call with %s, for addr: %s",rpcMethod, addr)
            return false
    }
}



