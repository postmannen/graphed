package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/postmannen/graphed"
)

func main() {
	// Example usage
	store := graphed.NewNodeStore[string]()

	// Add some nodes
	store.AddNode("root", "Root Value", "")
	store.AddNode("child1", "Child 1 Value", "root")
	store.AddNode("child2", "Child 2 Value", "root")
	store.AddNode("grandchild1", "Grandchild 1 Value", "child1")

	// Get and print node information
	if node, err := store.Node("child1"); err == nil {
		fmt.Printf("Node: %s\n", node.Name())
		fmt.Printf("Value: %s\n", node.Value())
		fmt.Printf("Parent: %s\n", node.Parent().Name())
		fmt.Printf("Children count: %d\n", len(node.Children()))
	}

	fmt.Println("--------------------------------")

	// Get and print node information
	if node, err := store.Node("root"); err == nil {
		fmt.Printf("Node: %s\n", node.Name())
		fmt.Printf("Value: %s\n", node.Value())
		fmt.Printf("Parent: %s\n", node.Parent().Name())
		fmt.Printf("Children count: %d\n", len(node.Children()))
	}

	fmt.Println("--------------------------------")

	// Marshal the entire store to JSON
	jsonData, err := json.MarshalIndent(store.AllNodes(), "", "    ")
	if err != nil {
		log.Fatalf("Error marshaling to JSON: %v\n", err)
	}
	fmt.Println(string(jsonData))
}
