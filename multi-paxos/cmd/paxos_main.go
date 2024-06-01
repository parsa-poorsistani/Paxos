package main

import (
    "flag"
    "strings"
    "github.com/parsa-poorsistani/Paxos/multi-paxos" 
)

func main() {
    // Command-line arguments
    id := flag.Int("id", 1, "ID of the Paxos node")
    peers := flag.String("peers", "localhost:1235,localhost:1236", "Comma-separated list of peer addresses")
    addr := flag.String("addr", "localhost:1234", "Address of this node")
    statePath := flag.String("state", "state.json", "Path to the state file")
    flag.Parse()

    // Split peers into a slice
    peerList := strings.Split(*peers, ",")

    // Create and start the Paxos node
    node := multipaxos.Make(*id, peerList, *addr, *statePath)

    _ = node

    // Prevent the main function from exiting
    select {}
}

