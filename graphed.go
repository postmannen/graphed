package graphed

import (
	"fmt"
	"time"

	"github.com/gofrs/uuid"
)

//type Grapher interface {
//	AddNode(name string, parentName string) error
//	Node(name string) (*Node, error)
//	AllNodes() map[uuid.UUID]*Node
//	AddToValues(name string, value []byte) error
//}

// Node in graph
type Node struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
	// TODO: Make use of timestamp
	Timestamp time.Time `json:"timestamp"`
	// Single value. Ex. single log line.
	Value []byte `json:"value"`
	// multiple values. Ex. multiple log lines.
	Values [][]byte `json:"values"`
	// A node can have multiple parents, and we use a map of map
	// to show the relationship with the parent, and what kind
	// of relationship it is.
	// The key of the parent map is the type of relationship.
	// A node can have f.ex. multiple parents of type "Linux",
	// and multiple parent of type "cloud"
	Parent map[string]map[uuid.UUID]struct{} `json:"parent,omitempty"`
	// HERE !!!!!!!!!!!!
	Children map[uuid.UUID]struct{} `json:"children,omitempty"`
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
func (ns *NodeStore) AddToValues(name string, value []byte) error {
	id := ns.nameToId[name]
	if id == uuid.Nil {
		return fmt.Errorf("node %s not found", name)
	}

	ns.nodes[id].Values = append(ns.nodes[id].Values, value)
	return nil
}

func newNode(name string, id uuid.UUID) *Node {
	n := Node{
		ID:       id,
		Name:     name,
		Values:   make([][]byte, 0),
		Parent:   make(map[string]map[uuid.UUID]struct{}),
		Children: make(map[uuid.UUID]struct{}),
	}

	return &n
}

// AddNode adds a new node to the store
//
// TODO: Add AddValue method to add a value to the node
// TODO: Using single parent in function arguments for now. Replace with slice later. Also add a parent relationship type.
func (ns *NodeStore) AddNode(name string, parentName string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}

	// store the name in the map to get the id of the node
	ns.nameToId[name] = id

	// Create new node
	newNode := newNode(name, id)

	var parentId uuid.UUID
	var parentExists bool

	if parentName != "" {
		parentId, parentExists = ns.nameToId[parentName]
	}

	// If parent name is provided, set up parent-child relationship
	if parentExists {
		// If there is a parent, add the new node to the parent's children
		parent := ns.nodes[parentId]

		parent.Children[id] = struct{}{}

		// Set the parent of the newNode
		m := make(map[uuid.UUID]struct{})
		m[parentId] = struct{}{}
		newNode.Parent["relationship"] = m
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

// // NodeJSON is a helper struct for JSON marshaling
// type NodeJSON struct {
// 	Name          string    `json:"name"`
// 	Timestamp     time.Time `json:"timestamp"`
// 	Value         []byte    `json:"value"`
// 	Values        [][]byte  `json:"values"`
// 	ParentName    string    `json:"parent,omitempty"`
// 	ChildrenNames []string  `json:"children,omitempty"`
// }
//
// // MarshalJSON implements custom JSON marshaling for Node
// func (n *Node) MarshalJSON() ([]byte, error) {
//
// 	nodeJSON := NodeJSON{
// 		Name:   n.Name(),
// 		Value:  n.Value(),
// 		Values: n.Values(),
// 	}
//
// 	if n.Parent() != nil {
// 		nodeJSON.ParentName = n.Parent().Name()
// 	}
//
// 	if len(n.Children()) > 0 {
// 		nodeJSON.ChildrenNames = make([]string, 0, len(n.Children()))
// 		for childId := range n.Children() {
// 			nodeJSON.ChildrenNames = append(nodeJSON.ChildrenNames, childId.String())
// 		}
// 	}
//
// 	return json.Marshal(nodeJSON)
// }
