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

	store.AddNode("customer1", "customer1-value", "root")

	store.AddNode("customer2", "customer2-value", "root")

	store.AddNode("customer2.location1", "customer2.location1-value", "customer2")

	store.AddNode("customer2.location2", "customer2.location2-value", "customer2")
	store.AddNode("customer2.location1.service1", "customer2.location1.service1-value", "customer2.location1")

	store.AddNode("customer2.location2.service1.logs", "customer2.location2.service1.logs-value", "customer2.location2.service1")
	store.AddNode("AD.logs", "AD log line 1", "customer2.location2.service1.logs")
	store.AddNode("AD.logs", "AD log line 2", "customer2.location2.service1.logs")
	store.AddNode("AD.logs", "AD log line 3", "customer2.location2.service1.logs")

	fmt.Println("--------------------------------")

	// Marshal the entire store to JSON
	jsonData, err := json.MarshalIndent(store.AllNodes(), "", "    ")
	if err != nil {
		log.Fatalf("Error marshaling to JSON: %v\n", err)
	}
	fmt.Println(string(jsonData))
}
