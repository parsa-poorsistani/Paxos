package main

import (
	"fmt"
	paxos "github.com/parsa-poorsistani/Paxos/basic-paxos"
	"time"
)

func main() {
	// Addresses for three nodes
	addresses := []string{"localhost:1234", "localhost:1235", "localhost:1236"}

	// Initialize three nodes
	nodeA := paxos.NewPaxosNode(1, []string{addresses[1], addresses[2]}, addresses[0], "./persistent-storage/node1.json")
	nodeB := paxos.NewPaxosNode(2, []string{addresses[0], addresses[2]}, addresses[1], "./persistent-storage/node2.json")
	nodeC := paxos.NewPaxosNode(3, []string{addresses[0], addresses[1]}, addresses[2], "./persistent-storage/node3.json")

	// Allow some time for the nodes to start
	time.Sleep(2 * time.Second)

	// Test: Node A proposes a value
	go func() {
		fmt.Println("Node 1 proposing value foo:bar")
		nodeA.Propose("foo:bar")
	}()

	// Allow some time for the proposal to complete
	time.Sleep(2 * time.Second)

	// Test: Node B proposes another value
	go func() {
		fmt.Println("Node 2 proposing value x:y")
		nodeB.Propose("x:y")
	}()

	// Allow some time for the proposal to complete
	time.Sleep(2 * time.Second)

	// Test: Node C proposes another value
	go func() {
		fmt.Println("Node 3 proposing value foo:too")
		nodeC.Propose("foo:too")
	}()

	// Allow some time for the proposal to complete
	time.Sleep(2 * time.Second)

	// Query the accepted values from all nodes
	go func() {
		valueA := nodeA.LearnValue()
		valueB := nodeB.LearnValue()
		valueC := nodeC.LearnValue()

		fmt.Printf("Node A learned value: %v\n", valueA)
		fmt.Printf("Node B learned value: %v\n", valueB)
		fmt.Printf("Node C learned value: %v\n", valueC)
	}()

	// Prevent the main function from exiting immediately
	select {}
}

