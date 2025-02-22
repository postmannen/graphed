package graphed

import (
	"encoding/json"
	"fmt"

	"github.com/gofrs/uuid"
)

// Node represents a node with a generic value in the graph
type Node[T any] struct {
	id       uuid.UUID
	name     string
	value    T
	parent   *Node[T]
	children map[uuid.UUID]*Node[T]
}

// Name returns the name of the node
func (n *Node[T]) Name() string {
	if n == nil {
		return ""
	}

	return n.name
}

// Value returns the value of the node
func (n *Node[T]) Value() T {
	return n.value
}

// Parent returns the parent of the node
func (n *Node[T]) Parent() *Node[T] {
	if n.parent == nil {
		return nil
	}

	return n.parent
}

// Children returns the children of the node
func (n *Node[T]) Children() map[uuid.UUID]*Node[T] {
	return n.children
}

// NodeStore represents the graph data store
type NodeStore[T any] struct {
	nodes    map[uuid.UUID]*Node[T]
	nameToId map[string]uuid.UUID
}

// NewNodeStore creates a new instance of NodeStore
func NewNodeStore[T any]() *NodeStore[T] {
	n := NodeStore[T]{
		nodes:    make(map[uuid.UUID]*Node[T]),
		nameToId: make(map[string]uuid.UUID),
	}

	return &n
}

// AddNode adds a new node to the store
func (ns *NodeStore[T]) AddNode(name string, value T, parentName string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}

	// store the name in the map to get the id of the node
	ns.nameToId[name] = id

	// Create new node
	newNode := &Node[T]{
		id:       id,
		name:     name,
		value:    value,
		children: make(map[uuid.UUID]*Node[T]),
	}

	var parentId uuid.UUID
	var parentExists bool
	if parentName != "" {
		parentId, parentExists = ns.nameToId[parentName]
	}

	// If parent name is provided, set up parent-child relationship
	if parentExists {
		// If there is a parent, add the new node to the parent's children
		parent := ns.nodes[parentId]

		parent.children[id] = newNode

		// Set the parent of the new node
		newNode.parent = parent
	}

	// Add node to store
	ns.nodes[id] = newNode
	return nil
}

// Node retrieves a node and its relationships from the store
func (ns *NodeStore[T]) Node(name string) (*Node[T], error) {
	if name == "" {
		return nil, fmt.Errorf("node %s name is empty", name)
	}

	id, exists := ns.nameToId[name]
	if !exists {
		return nil, fmt.Errorf("name to id mapping for node %s not found", name)
	}

	node, exists := ns.nodes[id]
	if !exists {
		return nil, fmt.Errorf("node %s not found in store", name)
	}
	return node, nil
}

// AllNodes returns all nodes in the store
func (ns *NodeStore[T]) AllNodes() map[uuid.UUID]*Node[T] {
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
		for childId := range n.Children() {
			nodeJSON.ChildrenNames = append(nodeJSON.ChildrenNames, childId.String())
		}
	}

	return json.Marshal(nodeJSON)
}
