package graphed

import (
	"fmt"
	"testing"
)

// benchmarkNodeStoreSize runs AddNode operations for n nodes
func benchmarkNodeStoreSize(n int, b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store := NewNodeStore()
		b.StartTimer()

		// Add root node
		store.AddNode("root", "")

		// Add n customer nodes
		for j := 0; j < n; j++ {
			customerName := fmt.Sprintf("customer%d", j)
			store.AddNode(customerName, "root")

			// Add locations for each customer
			for k := 0; k < 2; k++ {
				locationName := fmt.Sprintf("%s.location%d", customerName, k)
				store.AddNode(locationName, customerName)

				// Add services for each location
				serviceName := fmt.Sprintf("%s.service1", locationName)
				store.AddNode(serviceName, locationName)

				// Add logs for each service
				logsName := fmt.Sprintf("%s.logs", serviceName)
				store.AddNode(logsName, serviceName)

				// Add log entries
				for l := 0; l < 3; l++ {
					logEntryName := fmt.Sprintf("AD.logs%d", l)
					store.AddNode(logEntryName, logsName)
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
	store := NewNodeStore()
	store.AddNode("root", "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeName := fmt.Sprintf("node%d", i)
		store.AddNode(nodeName, "root")
	}
}

func BenchmarkNodeLookup(b *testing.B) {
	store := NewNodeStore()
	store.AddNode("root", "")
	store.AddNode("test_node1", "root")
	store.AddNode("test_node2", "root")
	store.AddNode("test_service", "test_node1")
	store.AddNode("test_service", "test_node1")
	store.AddNode("test_service", "test_node1")
	store.AddNode("test_service", "test_node1")
	store.AddNode("test_service", "test_node1")
	store.AddNode("test_service", "test_node1")
	store.AddNode("test_service", "test_node1")
	store.AddNode("test_service", "test_node1")
	store.AddNode("test_service", "test_node1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Node("test_node")
	}
}
