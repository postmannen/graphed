package graphed

import (
	"encoding/json"
	"fmt"

	"github.com/gofrs/uuid"
)

// Node represents a node with a generic value in the graph
type Node struct {
	id       uuid.UUID
	name     string
	value    [][]byte
	parent   *Node
	children map[uuid.UUID]*Node
}

// Name returns the name of the node
func (n *Node) Name() string {
	if n == nil {
		return ""
	}

	return n.name
}

// Value returns the value of the node
func (n *Node) Value() [][]byte {
	return n.value
}

// Parent returns the parent of the node
func (n *Node) Parent() *Node {
	if n.parent == nil {
		return nil
	}

	return n.parent
}

// Children returns the children of the node
func (n *Node) Children() map[uuid.UUID]*Node {
	return n.children
}

// NodeStore represents the graph data store
type NodeStore struct {
	nodes    map[uuid.UUID]*Node
	nameToId map[string]uuid.UUID
}

// NewNodeStore creates a new instance of NodeStore
func NewNodeStore() *NodeStore {
	n := NodeStore{
		nodes:    make(map[uuid.UUID]*Node),
		nameToId: make(map[string]uuid.UUID),
	}

	return &n
}

// AddValue adds a value to a node
func (ns *NodeStore) AddValue(name string, value []byte) error {
	id := ns.nameToId[name]
	if id == uuid.Nil {
		return fmt.Errorf("node %s not found", name)
	}

	ns.nodes[id].value = append(ns.nodes[id].value, value)
	return nil
}

// AddNode adds a new node to the store
//
// TODO: Add AddValue method to add a value to the node
func (ns *NodeStore) AddNode(name string, parentName string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}

	// store the name in the map to get the id of the node
	ns.nameToId[name] = id

	// Create new node
	newNode := &Node{
		id:       id,
		name:     name,
		value:    make([][]byte, 0),
		children: make(map[uuid.UUID]*Node),
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
func (ns *NodeStore) Node(name string) (*Node, error) {
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
func (ns *NodeStore) AllNodes() map[uuid.UUID]*Node {
	return ns.nodes
}

// NodeJSON is a helper struct for JSON marshaling
type NodeJSON struct {
	Name          string   `json:"name"`
	Value         [][]byte `json:"value"`
	ParentName    string   `json:"parent,omitempty"`
	ChildrenNames []string `json:"children,omitempty"`
}

// MarshalJSON implements custom JSON marshaling for Node
func (n *Node) MarshalJSON() ([]byte, error) {

	nodeJSON := NodeJSON{
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
