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
    // not sure about maxSeen
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
    InstanceIdx  int
    Proposal Proposal
}

type AcceptReply struct {
    Accepted bool
}

// RPC arguments and replies for Learn phase.
type LearnArgs struct {
    InstanceIdx int
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
        electionTimeout: time.Duration(1000 + rand.Intn(2001)) * time.Millisecond,
    }
    fmt.Printf("Node %d electionTimeoout: %d\n",node.id, node.electionTimeout)
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
        if pn.isLeader {
            continue
        }
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
    fmt.Printf("Node %d starting election\n", pn.id)
    pn.mu.Lock()
    pn.currentTerm++
    term := pn.currentTerm
    pn.mu.Unlock()
    
    votes := 1

    var voteMu sync.Mutex
    var voteWg sync.WaitGroup
    for _, peer := range pn.peers {
        voteWg.Add(1)
        fmt.Printf("Node %d requesting vote from Node %s\n",pn.id, peer)
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
        fmt.Printf("Node %d won the election(votes: %d) and is now leader\n", pn.id,votes)
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
                        InstanceIdx: idx, 
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

func(pn *PaxosNode) Accept(args AcceptArgs, reply *AcceptReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()
    
    instance, exists := pn.instances[args.InstanceIdx]
    if !exists {
        instance = &PaxosInstance{}
        pn.instances[args.InstanceIdx] = instance        
    }

    if args.Proposal.Number >= instance.maxSeen {
        instance.maxSeen = args.Proposal.Number
        instance.acceptedNum = args.Proposal.Number
        instance.acceptedValue = args.Proposal.Value
        reply.Accepted = true
        go pn.saveState()
    } else {
        reply.Accepted = false
    }
    return nil
}

func(pn *PaxosNode) Prepare(args PrepareArgs, reply *PrepareReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    instance, exists := pn.instances[args.InstanceIdx]
    if !exists {
        instance = &PaxosInstance{}
        pn.instances[args.InstanceIdx] = instance        
    }

    if args.ProposalNumber > instance.maxSeen {
        instance.maxSeen = args.ProposalNumber
        reply.Promise = true
    } else {
        reply.Promise = false
    }
    
    // Always return the highest accepted proposal details
    reply.AcceptedNum = instance.acceptedNum
    reply.AcceptedValue = instance.acceptedValue
    
    go pn.saveState()
    return nil
}
func(pn *PaxosNode) AddCommand(args AddCommandArgs, reply *AddCommandReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()
    if !pn.isLeader {
        // redirect to leader
        fmt.Printf("Node %d, is not leader, redirecting to %d\n", pn.id,pn.leaderID)
    }
    
    commandNum := len(pn.instances)
    for i := 0; i<commandNum; i++ {
        if instance, exists := pn.instances[i]; exists && instance.isAccepted{
            continue
        } else if exists && !instance.isAccepted {

            // Accept this log entry first
            proposal := Proposal{
                Number: instance.acceptedNum,
                Value: instance.acceptedValue,
            }
            
            acceptArgs := AcceptArgs {
                InstanceIdx: i,
                Proposal: proposal,
            }
            var acceptReply AcceptReply
            accepts := 1
            var peerWg sync.WaitGroup

            for _, peer := range pn.peers {
                peerWg.Add(1)
                go func(peer string) {
                    defer peerWg.Done()
                    if Call(peer,"PaxosNode.Accept", acceptArgs, &acceptReply) && acceptReply.Accepted {
                        pn.mu.Lock()
                        accepts++
                        pn.mu.Unlock()
                    }
                }(peer)
            }
            peerWg.Wait()

            if accepts > len(pn.peers)/2 {
                instance.isAccepted = true
                go pn.saveState()
            } else {
                return fmt.Errorf("command not accepted by majority\n")
            }
        } else {
            // Check with other nodes if they have an accepted value for this slot
            prepareArgs := PrepareArgs {
                ProposalNumber: pn.generateProposalNumber(),
                InstanceIdx: i, 
            }
            var prepareReply PrepareReply
            promises := 1
            var maxAcceptedValue interface{}
            var maxAcceptedNum int64 = -1
            var peerWg sync.WaitGroup

            for _, peer := range pn.peers {
                peerWg.Add(1)
                go func(peer string) {
                    defer peerWg.Done()
                    if Call(peer, "PaxosNode.Prepare", prepareArgs, &prepareReply) && prepareReply.Promise {
                        pn.mu.Lock()
                        promises++
                        if prepareReply.AcceptedNum > maxAcceptedNum {
                            maxAcceptedNum = prepareReply.AcceptedNum
                            maxAcceptedValue = prepareReply.AcceptedValue
                        }
                        pn.mu.Unlock()
                    }
                }(peer)
            }
            peerWg.Wait()

            if promises>len(pn.peers)/2 && maxAcceptedValue != nil {
                // Accept the value for this slot that other nodes have chosen before
                proposal := Proposal{
                    Number: maxAcceptedNum,
                    Value: maxAcceptedValue,
                }

                acceptArgs := AcceptArgs{
                    InstanceIdx: i,
                    Proposal: proposal,
                }
                var acceptReply AcceptReply
                accepts := 1
                var peerWg sync.WaitGroup

                for _, peer := range pn.peers {
                    peerWg.Add(1)
                    go func(peer string){
                        defer peerWg.Done()
                        if Call(peer, "PaxosNode.Accept", acceptArgs, &acceptReply) && acceptReply.Accepted {
                            pn.mu.Lock()
                            accepts++
                            pn.mu.Unlock()
                        }
                    }(peer)
                }
                peerWg.Wait()

                if accepts>len(pn.peers)/2 {
                    pn.instances[i] = &PaxosInstance{
                        maxSeen: proposal.Number,
                        acceptedNum: proposal.Number,
                        acceptedValue: proposal.Value,
                        isAccepted: true,
                    }
                    go pn.saveState()
                } else {
                    // need better error handling
                    fmt.Errorf("didn't accepted by the majority for ")
                }
            } else {
                // This is the first free slot and other 
                // nodes didn't have anything in this slot
                commandNum = i 
                break
            }
        }
    }
    // Propose New Command
    accepts := 1
    var peerWg sync.WaitGroup
    
    propsalNumer := pn.generateProposalNumber()
    proposal := Proposal{
        Number: propsalNumer,
        Value: args.Value,
    }
    acceptArgs := AcceptArgs{
        InstanceIdx: commandNum,
        Proposal: proposal,
    }
    var acceptReply AcceptReply

    for _, peer := range pn.peers {
        peerWg.Add(1)
        go func(peer string){
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
        pn.instances[commandNum] = &PaxosInstance{
            maxSeen: propsalNumer,
            acceptedNum: propsalNumer,
            acceptedValue: proposal.Value,
            isAccepted: true,
        }
        go pn.saveState()
        reply.CommandNum = commandNum
    } else {
        return fmt.Errorf("the new Command didn't accepted by the majority")
    }
    return nil
} 

func(pn *PaxosNode) sendHearbeats() {
    fmt.Printf("Node %d is starting to send heartbeats and the leader is %d\n",pn.id,pn.leaderID)
    for {
        pn.mu.Lock()
        if !pn.isLeader {
            return
        }
        currentTerm := pn.currentTerm
        pn.mu.Unlock()

        args := HeartbeatArgs{Term: currentTerm, LeaderID: pn.leaderID}
        var reply HeartbeatReply
        var wg sync.WaitGroup
        for _, peer := range pn.peers {
            fmt.Printf("Sending heartbeat from %d to %s\n",pn.id,peer)
            wg.Add(1)
            go func(peer string) {
                defer wg.Done()
                Call(peer, "PaxosNode.Heartbeat", args, &reply)
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

    fmt.Printf("Node %d requested vote from %d in term: %d\n",args.CandidateID,pn.id,args.Term)
    if args.Term > pn.currentTerm {
        pn.isLeader = false
        pn.currentTerm = args.Term
        reply.VoteGranted = true
    } else {
        reply.VoteGranted = false
    }

    return nil
}


func(pn *PaxosNode) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {
    pn.mu.Lock()
    defer pn.mu.Unlock()

    fmt.Printf("Heartbeat RPC received by node %d, from Node %d in term: %d\n",pn.id,args.LeaderID,args.Term)
    if args.Term >= pn.currentTerm {
        pn.leaderID = args.LeaderID
        pn.currentTerm = args.Term
        pn.isLeader = false
        pn.heartbeatChan <- true
    }
    return nil
}

func(pn *PaxosNode) ListCommands(args ListCommandsArgs, reply *ListCommandsReply) error {
    commands := make(map[int]interface{})
    for idx, instance := range pn.instances {
        if instance.isAccepted {
            commands[idx] = instance
        }
    }
    reply.Commands = commands
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
    case callDone := <- call.Done:
            if callDone.Error != nil {
                fmt.Printf("Call method error: %v\n", callDone.Error)
            }
            return call.Error == nil
        case <- time.After(1 * time.Second):
            fmt.Printf("timeout for RPC call with %s, for addr: %s\n",rpcMethod, addr)
            return false
    }
}

