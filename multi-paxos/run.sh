#!/bin/bash

cleanup() {
    echo "cleaning up ..."
    kill $PID1 $PID2 $PID3
    rm multipaxos_node client/client
    exit 0
}

# Trap SIGINT (Ctrl+C) and call the cleanup function
trap cleanup SIGINT SIGTERM
# Build the Multi-Paxos node and client
go build -o multipaxos_node ./cmd/paxos_main.go
go build -o client ./client/client.go

# Start the Multi-Paxos nodes
./multipaxos_node -id=1 -peers="localhost:1235,localhost:1236" -addr="localhost:1234" -state="state1.json" &
PID1=$!
./multipaxos_node -id=2 -peers="localhost:1234,localhost:1236" -addr="localhost:1235" -state="state2.json" &
PID2=$!
./multipaxos_node -id=3 -peers="localhost:1234,localhost:1235" -addr="localhost:1236" -state="state3.json" &
PID3=$!

# Allow some time for the nodes to start
sleep 5

# Run the client
./client/client

# Kill the Multi-Paxos nodes
sleep 60

cleanup
# Clean up


