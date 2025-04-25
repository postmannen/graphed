package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/postmannen/graphed"
)

func main() {
	// Example usage
	store := graphed.NewNodeStore()

	// Add some nodes
	store.AddNode("root", "")

	// Customer 1
	store.AddNode("customer1", "root")

	store.AddNode("customer1.location1", "customer1")
	store.AddNode("customer1.location1.service1", "customer1.location1")
	store.AddToValues("customer1.location1.service1", []byte("some data customer1 1"))
	store.AddToValues("customer1.location1.service1", []byte("some other data customer1 2"))
	store.AddToValues("customer1.location1.service1", []byte("even more data customer1 3"))
	// Customer 2
	store.AddNode("customer2", "root")

	store.AddNode("customer2.location1", "customer2")

	store.AddNode("customer2.location2", "customer2")
	store.AddNode("customer2.location1.service1", "customer2.location1")

	store.AddNode("customer2.location2.service1.logs", "customer2.location2.service1")
	store.AddNode("AD.logs", "customer2.location2.service1.logs")
	store.AddNode("AD.logs", "customer2.location2.service1.logs")
	store.AddNode("AD.logs", "customer2.location2.service1.logs")

	fmt.Println("--------------------------------")

	// Get a single node.
	node, err := store.Node("customer2.location2.service1.logs")
	if err != nil {
		log.Fatalf("Error getting node: %v\n", err)
	}
	fmt.Printf("Node: %+v\n", node)

	fmt.Println("--------------------------------")

	// Marshal the entire store to JSON.
	jsonData, err := json.MarshalIndent(store.AllNodes(), "", "    ")
	if err != nil {
		log.Fatalf("Error marshaling to JSON: %v\n", err)
	}
	fmt.Println(string(jsonData))
}
