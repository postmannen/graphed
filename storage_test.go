package graphed

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistentNodeStore(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "graphed-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up when done

	// Create data directory
	dataDir := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}

	// Test basic operations
	t.Run("BasicOperations", func(t *testing.T) {
		// Create a new store
		store, err := NewPersistentNodeStore(dataDir, WithChunkSize(5))
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}
		defer store.Close()

		// Add a root node
		if err := store.AddNode("root", ""); err != nil {
			t.Fatalf("Failed to add root node: %v", err)
		}

		// Add a child node
		if err := store.AddNode("child", "root"); err != nil {
			t.Fatalf("Failed to add child node: %v", err)
		}

		// Retrieve the child node
		child, err := store.GetNodeByName("child")
		if err != nil {
			t.Fatalf("Failed to retrieve child node: %v", err)
		}

		// Check the child node's parent
		r, ok := child.Parent["relationship"]
		if !ok {
			t.Fatalf("Child node's parent relationship is not set")
		}

		root, err := store.GetNodeByName("root")
		if err != nil {
			t.Fatalf("Failed to retrieve root node: %v", err)
		}

		for parentID := range r {
			if parentID != root.ID {
				t.Fatalf("Child node's parent is not set")
			}
		}

		// Add a value to the child node
		if err := store.AddToValues("child", []byte("test value")); err != nil {
			t.Fatalf("Failed to add value to child node: %v", err)
		}

		// Retrieve the child node again
		child, err = store.GetNodeByName("child")
		if err != nil {
			t.Fatalf("Failed to retrieve child node after adding value: %v", err)
		}

		// Check the value
		if len(child.Values) != 1 {
			t.Fatalf("Expected 1 value, got %d", len(child.Values))
		}
		if string(child.Values[0]) != "test value" {
			t.Fatalf("Expected value 'test value', got '%s'", string(child.Values[0]))
		}
	})

	// Test persistence
	t.Run("Persistence", func(t *testing.T) {
		// Create a new store
		store1, err := NewPersistentNodeStore(dataDir)
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}

		// Add a node
		if err := store1.AddNode("persistent", ""); err != nil {
			t.Fatalf("Failed to add node: %v", err)
		}

		// Add a value
		if err := store1.AddToValues("persistent", []byte("persistent value")); err != nil {
			t.Fatalf("Failed to add value: %v", err)
		}

		// Close the store
		if err := store1.Close(); err != nil {
			t.Fatalf("Failed to close store: %v", err)
		}

		// Create a new store with the same data directory
		store2, err := NewPersistentNodeStore(dataDir)
		if err != nil {
			t.Fatalf("Failed to create second store: %v", err)
		}
		defer store2.Close()

		// Retrieve the node
		node, err := store2.GetNodeByName("persistent")
		if err != nil {
			t.Fatalf("Failed to retrieve node from second store: %v", err)
		}

		// Check the value
		if len(node.Values) != 1 {
			t.Fatalf("Expected 1 value, got %d", len(node.Values))
		}
		if string(node.Values[0]) != "persistent value" {
			t.Fatalf("Expected value 'persistent value', got '%s'", string(node.Values[0]))
		}
	})

	// Test chunking
	t.Run("Chunking", func(t *testing.T) {
		// Create a new store with small chunk size
		store, err := NewPersistentNodeStore(dataDir, WithChunkSize(3))
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}
		defer store.Close()

		// Add a root node
		if err := store.AddNode("chunking-root", ""); err != nil {
			t.Fatalf("Failed to add root node: %v", err)
		}

		// Add enough nodes to create multiple chunks
		for i := 0; i < 10; i++ {
			nodeName := "chunking-child-" + string(rune('a'+i))
			if err := store.AddNode(nodeName, "chunking-root"); err != nil {
				t.Fatalf("Failed to add node %s: %v", nodeName, err)
			}
		}

		// Flush to disk
		if err := store.FlushAll(); err != nil {
			t.Fatalf("Failed to flush store: %v", err)
		}

		// Check that multiple chunk files were created
		files, err := os.ReadDir(dataDir)
		if err != nil {
			t.Fatalf("Failed to read data directory: %v", err)
		}

		chunkCount := 0
		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".json" && file.Name() != "metadata.json" {
				chunkCount++
			}
		}

		if chunkCount < 3 {
			t.Fatalf("Expected at least 3 chunk files, got %d", chunkCount)
		}

		// Verify we can still retrieve all nodes
		for i := 0; i < 10; i++ {
			nodeName := "chunking-child-" + string(rune('a'+i))
			_, err := store.GetNodeByName(nodeName)
			if err != nil {
				t.Fatalf("Failed to retrieve node %s: %v", nodeName, err)
			}
		}
	})

	// Test adapter
	t.Run("Adapter", func(t *testing.T) {
		// Create a new adapter
		adapter, err := NewPersistentNodeStore(dataDir)
		if err != nil {
			t.Fatalf("Failed to create adapter: %v", err)
		}
		defer adapter.Close()

		// Add a node
		if err := adapter.AddNode("adapter-node", ""); err != nil {
			t.Fatalf("Failed to add node through adapter: %v", err)
		}

		// Retrieve the node
		node, err := adapter.GetNodeByName("adapter-node")
		if err != nil {
			t.Fatalf("Failed to retrieve node through adapter: %v", err)
		}

		if node.Name != "adapter-node" {
			t.Fatalf("Expected node name 'adapter-node', got '%s'", node.Name)
		}

		// Add a value
		if err := adapter.AddToValues("adapter-node", []byte("adapter value")); err != nil {
			t.Fatalf("Failed to add value through adapter: %v", err)
		}

		// Retrieve the node again
		node, err = adapter.GetNodeByName("adapter-node")
		if err != nil {
			t.Fatalf("Failed to retrieve node after adding value: %v", err)
		}

		// Check the value
		if len(node.Values) != 1 {
			t.Fatalf("Expected 1 value, got %d", len(node.Values))
		}
		if string(node.Values[0]) != "adapter value" {
			t.Fatalf("Expected value 'adapter value', got '%s'", string(node.Values[0]))
		}
	})
}
