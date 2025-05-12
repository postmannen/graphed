package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/postmannen/graphed"
)

func main() {
	tempDir, err := os.MkdirTemp("", "graphed-example")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	fmt.Printf("Using data directory: %s\n", dataDir)

	store, err := graphed.NewNodeStore(dataDir, graphed.WithChunkSize(5))
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Add some nodes.........
	fmt.Println("Adding nodes...")
	if err := store.AddNode("root", ""); err != nil {
		log.Fatalf("Failed to add root node: %v", err)
	}

	if err := store.AddNode("customer1", "root"); err != nil {
		log.Fatalf("Failed to add customer1 node: %v", err)
	}

	if err := store.AddNode("customer2", "root"); err != nil {
		log.Fatalf("Failed to add customer2 node: %v", err)
	}

	fmt.Println("Retrieving a node...")
	node, err := store.GetNodeByName("customer1")
	if err != nil {
		log.Fatalf("Failed to retrieve customer1 node: %v", err)
	}

	fmt.Printf("Retrieved node: %s\n", node.Name)
	// fmt.Printf("Node values: %s\n", node.Values[0])

	fmt.Println("\nDebug info before closing:")
	debugInfo := store.DebugInfo()
	debugJSON, _ := json.MarshalIndent(debugInfo, "", "  ")
	fmt.Println(string(debugJSON))

	listAllNodes(store)

	// Close the store to and flush data to disk.
	fmt.Println("Closing store...")
	if err := store.Close(); err != nil {
		log.Fatalf("Failed to close store: %v", err)
	}

	fmt.Println("--------------------------------")

	// Reopen the store
	fmt.Println("\nReopening store to demonstrate persistence...")
	store2, err := graphed.NewNodeStore(dataDir, graphed.WithChunkSize(5))
	if err != nil {
		log.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()

	fmt.Println("\nDebug info after reopening:")
	debugInfo2 := store2.DebugInfo()
	debugJSON2, _ := json.MarshalIndent(debugInfo2, "", "  ")
	fmt.Println(string(debugJSON2))

	// Retrieve the first node again
	fmt.Println("\nRetrieving the first node ...........again...")
	node2, err := store2.GetNodeByName("customer1")
	if err != nil {
		log.Printf("Error: retrieving customer1 node after reopening: %v", err)
	}

	fmt.Printf("Retrieved node after reopening: %s\n", node2.Name)
	if len(node2.Values) > 0 {
		fmt.Printf("Node values after reopening: %s\n", node2.Values[0])
	}

	listAllNodes(store2)

}

func listAllNodes(store *graphed.NodeStore) {
	fmt.Println("---------Listing all nodes--------------")
	// List all nodes
	fmt.Println("\nListing all nodes:")
	allNodes, err := store.LoadAllNodes()
	if err != nil {
		log.Fatalf("Failed to load all nodes: %v", err)
	}
	fmt.Printf("Total nodes: %d\n", len(allNodes))
	for id, node := range allNodes {
		fmt.Printf("Node ID: %s, Name: %s, Parents: %v, Children: %v\n", id, node.Name, node.Parent, node.Children)
		if len(node.Children) > 0 {
			for rel, idMap := range node.Children {
				fmt.Printf("DEBUG:    Relationship: %s, Children: %v\n", rel, idMap)
			}
		}
	}
	fmt.Println("--------------------------------")
}
