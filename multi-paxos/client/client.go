package main

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
)

type AddCommandArgs struct {
    Value interface{}
}

type AddCommandReply struct {
    CommandNum int
}

type ListCommandsArgs struct{}

type ListCommandsReply struct {
    Commands map[int]interface{}
}

func addCommand(client *rpc.Client, value interface{}) {
    args := AddCommandArgs{Value: value}
    var reply AddCommandReply
    err := client.Call("PaxosNode.AddCommand", args, &reply)
    if err != nil {
        log.Printf("AddCommand error: %v\n", err)
    } else {
        log.Printf("Command added with number: %d\n", reply.CommandNum)
    }
}

func listCommands(client *rpc.Client) {
    args := ListCommandsArgs{}
    var reply ListCommandsReply
    err := client.Call("PaxosNode.ListCommands", args, &reply)
    if err != nil {
        log.Printf("ListCommands error: %v\n", err)
    } else {
        log.Printf("Chosen commands: %v\n", reply.Commands)
    }
}

func main() {
    clients := []string{"localhost:1234", "localhost:1235", "localhost:1236"}

    fmt.Println("client code started")
    for _, addr := range clients {
        client, err := rpc.Dial("tcp", addr)
        if err != nil {
            log.Fatalf("Failed to connect to server at %s: %v\n", addr, err)
        }
        defer client.Close()

        fmt.Println("client sending add command request")
        addCommand(client, "foo:bar")
        time.Sleep(1 * time.Second)

        addCommand(client, "x:y")
        time.Sleep(1 * time.Second)

        addCommand(client, "foo:too")
        time.Sleep(1 * time.Second)

        listCommands(client)
    }
}
