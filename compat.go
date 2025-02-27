package graphed

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/uuid"
)

// NodeStoreAdapter adapts the PersistentNodeStore to the NodeStore interface
// This allows existing code to use the persistent store without changes
type NodeStoreAdapter struct {
	persistentStore *PersistentNodeStore
}

// NewNodeStoreAdapter creates a new adapter for the persistent store
func NewNodeStoreAdapter(dataDir string, options ...StoreOption) (*NodeStoreAdapter, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create persistent store
	store, err := NewPersistentNodeStore(dataDir, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create persistent store: %w", err)
	}

	return &NodeStoreAdapter{
		persistentStore: store,
	}, nil
}

// AddNode adds a new node to the store
func (a *NodeStoreAdapter) AddNode(name string, parentName string) error {
	return a.persistentStore.AddNode(name, parentName)
}

// Node retrieves a node by name
func (a *NodeStoreAdapter) Node(name string) (*Node, error) {
	return a.persistentStore.Node(name)
}

// AddToValues adds a value to a node's values
func (a *NodeStoreAdapter) AddToValues(name string, value []byte) error {
	return a.persistentStore.AddToValues(name, value)
}

// AllNodes returns all nodes in the store
// Note: This returns metadata only, not the full nodes with values
func (a *NodeStoreAdapter) AllNodes() map[uuid.UUID]*Node {
	// Load all nodes from disk
	nodes, err := a.persistentStore.LoadAllNodes()
	if err != nil {
		// In case of error, return an empty map
		return make(map[uuid.UUID]*Node)
	}
	return nodes
}

// Close closes the store and ensures all data is flushed to disk
func (a *NodeStoreAdapter) Close() error {
	return a.persistentStore.Close()
}

// DefaultDataDir returns the default data directory for the persistent store
func DefaultDataDir() (string, error) {
	// Get user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home directory: %w", err)
	}

	// Create default data directory path
	dataDir := filepath.Join(homeDir, ".graphed")
	return dataDir, nil
}

// DebugInfo returns diagnostic information about the store
func (a *NodeStoreAdapter) DebugInfo() map[string]interface{} {
	return a.persistentStore.DebugInfo()
}
