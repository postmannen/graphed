package graphed

import (
	"fmt"
	"testing"
)

// benchmarkNodeStoreSize runs AddNode operations for n nodes
func benchmarkNodeStoreSize(n int, b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store := NewNodeStore[string]()
		b.StartTimer()

		// Add root node
		store.AddNode("root", "Root Value", "")

		// Add n customer nodes
		for j := 0; j < n; j++ {
			customerName := fmt.Sprintf("customer%d", j)
			store.AddNode(customerName, customerName+"-value", "root")

			// Add locations for each customer
			for k := 0; k < 2; k++ {
				locationName := fmt.Sprintf("%s.location%d", customerName, k)
				store.AddNode(locationName, locationName+"-value", customerName)

				// Add services for each location
				serviceName := fmt.Sprintf("%s.service1", locationName)
				store.AddNode(serviceName, serviceName+"-value", locationName)

				// Add logs for each service
				logsName := fmt.Sprintf("%s.logs", serviceName)
				store.AddNode(logsName, logsName+"-value", serviceName)

				// Add log entries
				for l := 0; l < 3; l++ {
					logEntryName := fmt.Sprintf("AD.logs%d", l)
					store.AddNode(logEntryName, fmt.Sprintf("AD log line %d", l), logsName)
				}
			}
		}
	}
}

// Benchmark different sizes of node trees
func BenchmarkNodeStore10(b *testing.B)    { benchmarkNodeStoreSize(10, b) }
func BenchmarkNodeStore100(b *testing.B)   { benchmarkNodeStoreSize(100, b) }
func BenchmarkNodeStore1000(b *testing.B)  { benchmarkNodeStoreSize(1000, b) }
func BenchmarkNodeStore10000(b *testing.B) { benchmarkNodeStoreSize(10000, b) }

// Benchmark individual operations
func BenchmarkAddNode(b *testing.B) {
	store := NewNodeStore[string]()
	store.AddNode("root", "Root Value", "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeName := fmt.Sprintf("node%d", i)
		store.AddNode(nodeName, "test value", "root")
	}
}

func BenchmarkNodeLookup(b *testing.B) {
	store := NewNodeStore[string]()
	store.AddNode("root", "Root Value", "")
	store.AddNode("test_node1", "test node value1", "root")
	store.AddNode("test_node2", "test node value2", "root")
	store.AddNode("test_service", "test service value 1", "test_node1")
	store.AddNode("test_service", "test service value 2", "test_node1")
	store.AddNode("test_service", "test service value 3", "test_node1")
	store.AddNode("test_service", "test service value 4", "test_node1")
	store.AddNode("test_service", "test service value 5", "test_node1")
	store.AddNode("test_service", "test service value 6", "test_node1")
	store.AddNode("test_service", "test service value 7", "test_node1")
	store.AddNode("test_service", "test service value 8", "test_node1")
	store.AddNode("test_service", "test service value 9", "test_node1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Node("test_node")
	}
}
