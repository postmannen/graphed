package graphed

import (
	"encoding/json"
	"fmt"
)

// Node represents a node with a generic value in the graph
type Node[T any] struct {
	name     string
	value    T
	parent   *Node[T]
	children map[string]*Node[T]
}

func (n *Node[T]) Name() string {
	if n == nil {
		return ""
	}

	return n.name
}

func (n *Node[T]) Value() T {
	return n.value
}

func (n *Node[T]) Parent() *Node[T] {
	if n.parent == nil {
		return nil
	}

	return n.parent
}

func (n *Node[T]) Children() map[string]*Node[T] {
	return n.children
}

// NodeStore represents the graph data store
type NodeStore[T any] struct {
	nodes map[string]*Node[T]
}

// NewNodeStore creates a new instance of NodeStore
func NewNodeStore[T any]() *NodeStore[T] {
	return &NodeStore[T]{
		nodes: make(map[string]*Node[T]),
	}
}

// AddNode adds a new node to the store
func (ns *NodeStore[T]) AddNode(name string, value T, parentName string) error {
	// Create new node
	newNode := &Node[T]{
		name:     name,
		value:    value,
		children: make(map[string]*Node[T]),
	}

	// If parent name is provided, set up parent-child relationship
	if parentName != "" {
		parent, exists := ns.nodes[parentName]
		if !exists {
			return fmt.Errorf("parent node %s not found", parentName)
		}
		newNode.parent = parent
		parent.children[name] = newNode
	}

	// Add node to store
	ns.nodes[name] = newNode
	return nil
}

// GetNode retrieves a node and its relationships from the store
func (ns *NodeStore[T]) Node(name string) (*Node[T], error) {
	node, exists := ns.nodes[name]
	if !exists {
		return nil, fmt.Errorf("node %s not found", name)
	}
	return node, nil
}

// GetAllNodes returns all nodes in the store
func (ns *NodeStore[T]) AllNodes() map[string]*Node[T] {
	return ns.nodes
}

// NodeJSON is a helper struct for JSON marshaling
type NodeJSON[T any] struct {
	Name          string   `json:"name"`
	Value         T        `json:"value"`
	ParentName    string   `json:"parent,omitempty"`
	ChildrenNames []string `json:"children,omitempty"`
}

// MarshalJSON implements custom JSON marshaling for Node
func (n *Node[T]) MarshalJSON() ([]byte, error) {
	nodeJSON := NodeJSON[T]{
		Name:  n.Name(),
		Value: n.Value(),
	}

	if n.Parent() != nil {
		nodeJSON.ParentName = n.Parent().Name()
	}

	if len(n.Children()) > 0 {
		nodeJSON.ChildrenNames = make([]string, 0, len(n.Children()))
		for childName := range n.Children() {
			nodeJSON.ChildrenNames = append(nodeJSON.ChildrenNames, childName)
		}
	}

	return json.Marshal(nodeJSON)
}
