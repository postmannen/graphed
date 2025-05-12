package graphed

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/uuid"
)

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
	Parent map[relationship]map[uuid.UUID]struct{} `json:"parent,omitempty"`
	// HERE !!!!!!!!!!!!
	Children map[relationship]map[uuid.UUID]struct{} `json:"children,omitempty"`
}

func newNode(name string, id uuid.UUID) *Node {
	n := Node{
		ID:       id,
		Name:     name,
		Values:   make([][]byte, 0),
		Parent:   make(map[relationship]map[uuid.UUID]struct{}),
		Children: make(map[relationship]map[uuid.UUID]struct{}),
	}

	return &n
}

type relationship string

// ChunkSize, how many nodes are stored in a single chunk file
const DefaultChunkSize = 100

// ChunkLocation represents where a node is stored on disk
type ChunkLocation struct {
	ChunkID int `json:"chunk_id"`
}

// NodeMetadata to use with in-memory lookup.
type NodeMetadata struct {
	ID       uuid.UUID                               `json:"id"`
	Parent   map[relationship]map[uuid.UUID]struct{} `json:"parent,omitempty"`
	Children map[relationship]map[uuid.UUID]struct{} `json:"children,omitempty"`
}

func newNodeMetadata(id uuid.UUID) *NodeMetadata {
	return &NodeMetadata{
		ID:       id,
		Parent:   make(map[relationship]map[uuid.UUID]struct{}),
		Children: make(map[relationship]map[uuid.UUID]struct{}),
	}
}

// NodeStore, NodeStore with disk persistence.
type NodeStore struct {
	// In-memory index
	nodes map[uuid.UUID]*NodeMetadata
	// In-memory index
	nameToID map[string]uuid.UUID
	// Map of each node's UUID to its physical location on disk.
	nodeToChunk  map[uuid.UUID]ChunkLocation
	chunks       map[int]*Chunk
	nextChunkID  int
	currentChunk *Chunk
	chunkSize    int
	dataDir      string
	mu           sync.RWMutex
	wal          *WriteAheadLog
}

// Chunk represents a collection of nodes stored together
type Chunk struct {
	ID       int                 `json:"id"`
	Nodes    map[uuid.UUID]*Node `json:"nodes"`
	Modified bool                `json:"-"`
	mu       sync.RWMutex        `json:"-"`
}

// WriteAheadLog handles durability and crash recovery
type WriteAheadLog struct {
	file    *os.File
	mu      sync.Mutex
	dataDir string
}

// WALEntry represents a single entry in the write-ahead log
type WALEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Operation string    `json:"operation"` // "add", "update", "delete"
	NodeID    uuid.UUID `json:"node_id"`
	Node      *Node     `json:"node,omitempty"`
}

// NewNodeStore creates a new NodeStore
func NewNodeStore(dataDir string, options ...StoreOption) (*NodeStore, error) {

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	store := &NodeStore{
		nodes:       make(map[uuid.UUID]*NodeMetadata),
		nameToID:    make(map[string]uuid.UUID),
		nodeToChunk: make(map[uuid.UUID]ChunkLocation),
		chunks:      make(map[int]*Chunk),
		nextChunkID: 1,
		chunkSize:   DefaultChunkSize,
		dataDir:     dataDir,
	}

	for _, option := range options {
		option(store)
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize WAL
	wal, err := newWriteAheadLog(dataDir)
	if err != nil {
		return nil, fmt.Errorf("NewPersistentNodeStore: failed to initialize WAL: %w", err)
	}
	store.wal = wal

	// Create initial chunk
	store.currentChunk = &Chunk{
		ID:       store.nextChunkID,
		Nodes:    make(map[uuid.UUID]*Node),
		Modified: false,
	}
	store.chunks[store.nextChunkID] = store.currentChunk
	store.nextChunkID++

	if err := store.loadMetadata(); err != nil {
		return nil, fmt.Errorf("NewPersistentNodeStore: failed to load metadata: %w", err)
	}

	if err := store.RecoverFromWAL(); err != nil {
		return nil, fmt.Errorf("NewPersistentNodeStore: failed to recover from WAL: %w", err)
	}

	return store, nil
}

// StoreOption, custom store configuration
type StoreOption func(*NodeStore)

// WithChunkSize to set the chunk size
func WithChunkSize(size int) StoreOption {
	return func(s *NodeStore) {
		if size > 0 {
			s.chunkSize = size
		}
	}
}

// newWriteAheadLog creates a new WAL
func newWriteAheadLog(dataDir string) (*WriteAheadLog, error) {
	walPath := filepath.Join(dataDir, "wal.log")
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("newWriteAheadLog: failed to open WAL file: %w", err)
	}

	return &WriteAheadLog{
		file:    file,
		dataDir: dataDir,
	}, nil
}

// AddNode adds a new node to the store
func (p *NodeStore) AddNode(name string, parentName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Generate UUID
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("AddNode: failed to generate UUID: %w", err)
	}

	newNode := newNode(name, id)

	metadata := newNodeMetadata(id)

	// Handle parent relationship
	var parentID uuid.UUID
	var parentExists bool
	if parentName != "" {
		parentID, parentExists = p.nameToID[parentName]
		if !parentExists {
			return fmt.Errorf("parent node %s not found", parentName)
		}

		// Update parent's children
		parentMeta := p.nodes[parentID]
		parentMeta.Children["relationship"] = make(map[uuid.UUID]struct{})
		parentMeta.Children["relationship"][id] = struct{}{}

		fmt.Printf("DEBUG: parentMeta for %s: %v\n", parentName, parentMeta.Children["relationship"])

		// Update parent node in storage
		if err := p.updateNodeInStorage(parentID); err != nil {
			return fmt.Errorf("AddNode: failed to update parent node: %w", err)
		}

		// Set parent in new node
		m := make(map[uuid.UUID]struct{})
		m[parentID] = struct{}{}
		newNode.Parent["relationship"] = m
		metadata.Parent["relationship"] = m
	}

	// Add to WAL
	if err := p.wal.LogAddNode(newNode); err != nil {
		return fmt.Errorf("AddNode: failed to log node addition: %w", err)
	}

	// Add to current chunk
	p.currentChunk.mu.Lock()
	p.currentChunk.Nodes[id] = newNode
	p.currentChunk.Modified = true

	// Record location
	location := ChunkLocation{
		ChunkID: p.currentChunk.ID,
	}
	p.nodeToChunk[id] = location
	p.currentChunk.mu.Unlock()

	// Add to in-memory indexes
	p.nodes[id] = metadata
	p.nameToID[name] = id

	// Check if current chunk is full
	if len(p.currentChunk.Nodes) >= p.chunkSize {
		// Flush current chunk to disk
		if err := p.flushChunk(p.currentChunk.ID); err != nil {
			return fmt.Errorf("AddNode: failed to flush chunk: %w", err)
		}

		// Create new chunk
		p.currentChunk = &Chunk{
			ID:       p.nextChunkID,
			Nodes:    make(map[uuid.UUID]*Node),
			Modified: false,
		}
		p.chunks[p.nextChunkID] = p.currentChunk
		p.nextChunkID++
	}

	return nil
}

// updateNodeInStorage updates a node in its chunk
func (p *NodeStore) updateNodeInStorage(id uuid.UUID) error {
	// Find the chunk containing this node
	location, exists := p.nodeToChunk[id]
	if !exists {
		return fmt.Errorf("updateNodeInStorage: node %s not found in chunk mapping", id)
	}

	// Check if the chunk exists.
	chunk, exists := p.chunks[location.ChunkID]
	if !exists {
		// Load the chunk from disk
		var err error
		chunk, err = p.loadChunk(location.ChunkID)
		if err != nil {
			return fmt.Errorf("updateNodeInStorage: failed to load chunk %d: %w", location.ChunkID, err)
		}
		p.chunks[location.ChunkID] = chunk
	}

	// Get the node from the chunk.
	node, err := p.getNodeFromChunk(id, chunk)
	if err != nil {
		return fmt.Errorf("updateNodeInStorage: failed to get node from chunk: %w", err)
	}

	// Update the from the node metadata
	metadata := p.nodes[id]
	node.Children = metadata.Children
	fmt.Printf("--- DEBUG, updateNodeInStorage: node.Children for %s: %v\n", node.Name, node.Children)

	// Mark chunk as modified
	// TODO: Add date here for when it was updated.
	chunk.mu.Lock()
	chunk.Modified = true
	chunk.mu.Unlock()

	// Log the update
	if err := p.wal.LogUpdateNode(node); err != nil {
		return fmt.Errorf("updateNodeInStorage: failed to log node update: %w", err)
	}

	return nil
}

// getNodeFromChunk gets a node from a chunk.
func (p *NodeStore) getNodeFromChunk(id uuid.UUID, chunk *Chunk) (*Node, error) {
	chunk.mu.RLock()
	defer chunk.mu.RUnlock()

	node, exists := chunk.Nodes[id]
	if !exists {
		return nil, fmt.Errorf("getNodeFromChunk: node %s not found in chunk %d", id, chunk.ID)
	}

	return node, nil
}

// loadChunk loads a chunk from disk
func (p *NodeStore) loadChunk(chunkID int) (*Chunk, error) {
	chunkPath := filepath.Join(p.dataDir, fmt.Sprintf("chunk_%d.json", chunkID))

	// Check if file exists
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("loadChunk: chunk file %s does not exist", chunkPath)
	}

	// Read file
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("loadChunk: failed to read chunk file: %w", err)
	}

	var chunk Chunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		return nil, fmt.Errorf("loadChunk: failed to unmarshal chunk: %w", err)
	}

	// TODO: Check if this is set before unmarshalling.
	//       Adding a check to detect if it might be a
	//       scenario before removing the code.
	//
	// chunk.ID = chunkID
	if chunk.ID == 0 {
		log.Fatalf("loadChunk: chunk ID is not set")
	}

	return &chunk, nil
}

// flushChunk writes a chunk to disk
func (ps *NodeStore) flushChunk(chunkID int) error {
	chunk, exists := ps.chunks[chunkID]
	if !exists {
		return fmt.Errorf("flushChunk: chunk %d not found", chunkID)
	}

	chunk.mu.RLock()
	defer chunk.mu.RUnlock()

	// Skip if not modified
	if !chunk.Modified {
		return nil
	}

	// Marshal to JSON
	data, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("flushChunk: failed to marshal chunk: %w", err)
	}

	// Write to file
	chunkPath := filepath.Join(ps.dataDir, fmt.Sprintf("chunk_%d.json", chunkID))
	if err := os.WriteFile(chunkPath, data, 0644); err != nil {
		return fmt.Errorf("flushChunk: failed to write chunk file: %w", err)
	}

	// Mark as not modified
	chunk.Modified = false

	return nil
}

// FlushAll writes all modified chunks to disk
func (p *NodeStore) FlushAll() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for chunkID := range p.chunks {
		if err := p.flushChunk(chunkID); err != nil {
			return fmt.Errorf("FlushAll: failed to flush chunk %d: %w", chunkID, err)
		}
	}

	// Save metadata
	if err := p.saveMetadata(); err != nil {
		return fmt.Errorf("FlushAll: failed to save metadata: %w", err)
	}

	return nil
}

// saveMetadata saves the store's metadata to disk
func (p *NodeStore) saveMetadata() error {
	metadataPath := filepath.Join(p.dataDir, "metadata.json")

	// Create metadata structure
	metadata := struct {
		Nodes       map[uuid.UUID]*NodeMetadata `json:"nodes"`
		NameToID    map[string]uuid.UUID        `json:"name_to_id"`
		NodeToChunk map[uuid.UUID]ChunkLocation `json:"node_to_chunk"`
		NextChunkID int                         `json:"next_chunk_id"`
	}{
		Nodes:       p.nodes,
		NameToID:    p.nameToID,
		NodeToChunk: p.nodeToChunk,
		NextChunkID: p.nextChunkID,
	}

	// Marshal to JSON
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("saveMetadata: failed to marshal metadata: %w", err)
	}

	// Write to file
	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	return nil
}

// loadMetadata loads the store's metadata from disk
func (p *NodeStore) loadMetadata() error {
	metadataPath := filepath.Join(p.dataDir, "metadata.json")

	// Check if file exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		// No metadata file, starting fresh
		return nil
	}

	// Read file
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Unmarshal
	var metadata struct {
		Nodes       map[uuid.UUID]*NodeMetadata `json:"nodes"`
		NameToID    map[string]uuid.UUID        `json:"name_to_id"`
		NodeToChunk map[uuid.UUID]ChunkLocation `json:"node_to_chunk"`
		NextChunkID int                         `json:"next_chunk_id"`
	}

	if err := json.Unmarshal(data, &metadata); err != nil {
		return fmt.Errorf("loadMetadata: failed to unmarshal metadata: %w", err)
	}

	// Apply metadata
	p.nodes = metadata.Nodes
	p.nameToID = metadata.NameToID
	p.nodeToChunk = metadata.NodeToChunk
	p.nextChunkID = metadata.NextChunkID

	return nil
}

// GetNodeByName retrieves a node by name
func (p *NodeStore) GetNodeByName(name string) (*Node, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if name == "" {
		return nil, fmt.Errorf("GetNodeByName: node name is empty")
	}

	// Get node ID
	id, exists := p.nameToID[name]
	if !exists {
		return nil, fmt.Errorf("GetNodeByName: node %s not found", name)
	}

	node, err := p.GetNodeByID(id)
	if err != nil {
		return nil, fmt.Errorf("GetNodeByName: node %s: %w", name, err)
	}

	return node, nil
}

func (p *NodeStore) GetNodeByID(id uuid.UUID) (*Node, error) {
	// Get node location
	location, exists := p.nodeToChunk[id]
	if !exists {
		return nil, fmt.Errorf("getNodeByID: node %s location not found", id)
	}

	// Get chunk
	chunk, exists := p.chunks[location.ChunkID]
	if !exists {
		// Load chunk from disk
		var err error
		chunk, err = p.loadChunk(location.ChunkID)
		if err != nil {
			return nil, fmt.Errorf("getNodeByID: failed to load chunk %d: %w", location.ChunkID, err)
		}
		p.chunks[location.ChunkID] = chunk
	}

	// Get node from chunk
	node, err := p.getNodeFromChunk(id, chunk)
	if err != nil {
		// If the node is not found in the chunk, there might be a mismatch between
		// the metadata and the actual chunk data. Let's try to recover by checking
		// all loaded chunks.
		for chunkID, c := range p.chunks {
			if chunkID == location.ChunkID {
				continue // Already checked this one
			}

			c.mu.RLock()
			n, exists := c.Nodes[id]
			c.mu.RUnlock()

			if exists {
				// Found the node in a different chunk, update the location
				p.nodeToChunk[id] = ChunkLocation{
					ChunkID: chunkID,
				}

				// Return the found node
				return n, nil
			}
		}

		// If we still can't find it, return the original error
		return nil, fmt.Errorf("getNodeByID: failed to get node from chunk: %w", err)
	}

	return node, nil
}

// GetAllChildNodes retrieves all child nodes of a node.
func (p *NodeStore) GetAllChildNodes(name string) ([]*Node, error) {
	n, err := p.GetNodeByName(name)
	if err != nil {
		return nil, fmt.Errorf("GetAllChildNodes: %w", err)
	}

	children := make([]*Node, 0, len(n.Children))

	// Range over the relationships.
	// TODO: Since the value of a relationship for now is set to "relationship",
	// we don't use the key. This needs to be changed in the future when realtionships
	// can have different values.
	for _, idMap := range n.Children {

		for childID := range idMap {
			child, err := p.GetNodeByID(childID)
			if err != nil {
				return nil, fmt.Errorf("GetAllChildNodes: %w", err)
			}
			children = append(children, child)
		}
	}

	return children, nil
}

// GetNodeParent retrieves the parent of a node
func (p *NodeStore) GetNodeParents(name string) ([]*Node, error) {
	n, err := p.GetNodeByName(name)
	if err != nil {
		return nil, fmt.Errorf("GetNodeParents: %w", err)
	}

	parents := make([]*Node, 0, len(n.Parent))

	for parentID := range n.Parent["relationship"] {
		parent, err := p.GetNodeByID(parentID)
		if err != nil {
			return nil, fmt.Errorf("GetNodeParents: %w", err)
		}
		parents = append(parents, parent)
	}

	return parents, nil
}

// AllNodes returns all nodes in the store
// Note: This returns metadata only, not the full nodes with values
// TODO: Check if we need this!!!
func (p *NodeStore) AllNodesMetadata() map[uuid.UUID]*Node {
	// Load all nodes from disk
	nodes, err := p.LoadAllNodes()
	if err != nil {
		// In case of error, return an empty map
		return make(map[uuid.UUID]*Node)
	}
	return nodes
}

// AddToValues adds a value to a node's values
func (p *NodeStore) AddToValues(name string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Get node ID
	id, exists := p.nameToID[name]
	if !exists {
		return fmt.Errorf("AddToValues: node %s not found", name)
	}

	// Get node location
	location, exists := p.nodeToChunk[id]
	if !exists {
		return fmt.Errorf("AddToValues: node %s location not found", name)
	}

	// Get chunk
	chunk, exists := p.chunks[location.ChunkID]
	if !exists {
		// Load chunk from disk
		var err error
		chunk, err = p.loadChunk(location.ChunkID)
		if err != nil {
			return fmt.Errorf("AddToValues: failed to load chunk %d: %w", location.ChunkID, err)
		}
		p.chunks[location.ChunkID] = chunk
	}

	// Get node from chunk
	node, err := p.getNodeFromChunk(id, chunk)
	if err != nil {
		return fmt.Errorf("AddToValues: failed to get node from chunk: %w", err)
	}

	// Add value
	node.Values = append(node.Values, value)

	// Mark chunk as modified
	chunk.mu.Lock()
	chunk.Modified = true
	chunk.mu.Unlock()

	// Log the update
	if err := p.wal.LogUpdateNode(node); err != nil {
		return fmt.Errorf("AddToValues: failed to log node update: %w", err)
	}

	return nil
}

// LogAddNode logs a node addition to the WAL
func (w *WriteAheadLog) LogAddNode(node *Node) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := WALEntry{
		Timestamp: time.Now(),
		Operation: "add",
		NodeID:    node.ID,
		Node:      node,
	}

	return w.writeEntry(entry)
}

// LogUpdateNode logs a node update to the WAL
func (w *WriteAheadLog) LogUpdateNode(node *Node) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := WALEntry{
		Timestamp: time.Now(),
		Operation: "update",
		NodeID:    node.ID,
		Node:      node,
	}

	return w.writeEntry(entry)
}

// writeEntry writes an entry to the WAL
func (w *WriteAheadLog) writeEntry(entry WALEntry) error {
	// Marshal entry
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("writeEntry: failed to marshal WAL entry: %w", err)
	}

	// Write length prefix
	lenBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenBuf, uint64(len(data)))

	if _, err := w.file.Write(lenBuf); err != nil {
		return fmt.Errorf("writeEntry: failed to write entry length: %w", err)
	}

	// Write data
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("writeEntry: failed to write entry data: %w", err)
	}

	// Sync to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("writeEntry: failed to sync WAL: %w", err)
	}

	return nil
}

// Close closes the store and ensures all data is flushed to disk
func (p *NodeStore) Close() error {
	// Flush all chunks
	if err := p.FlushAll(); err != nil {
		return fmt.Errorf("Close: failed to flush all chunks: %w", err)
	}

	// Close WAL
	if err := p.wal.file.Close(); err != nil {
		return fmt.Errorf("Close: failed to close WAL: %w", err)
	}

	return nil
}

// AllNodes returns all nodes in the store (metadata only)
func (p *NodeStore) AllNodes() map[uuid.UUID]*NodeMetadata {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.nodes
}

// LoadAllNodes loads all nodes from disk into memory.
//
// The nodes are stored in chunks on disk, and the nodeToChunk map
// contains the location of each node in the correct chunk.
func (p *NodeStore) LoadAllNodes() (map[uuid.UUID]*Node, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[uuid.UUID]*Node)

	// Iterate through all node chunk locations
	for id, location := range p.nodeToChunk {
		// With each chunk...
		chunk, exists := p.chunks[location.ChunkID]
		if !exists {
			var err error
			chunk, err = p.loadChunk(location.ChunkID)
			if err != nil {
				return nil, fmt.Errorf("LoadAllNodes: failed to load chunk %d: %w", location.ChunkID, err)
			}
			p.chunks[location.ChunkID] = chunk
		}

		// Get node from chunk
		node, err := p.getNodeFromChunk(id, chunk)
		if err != nil {
			return nil, fmt.Errorf("LoadAllNodes: failed to get node %s: %w", id, err)
		}

		result[id] = node
	}

	return result, nil
}

// RecoverFromWAL recovers the store state from the WAL
// This is called during initialization if needed
//
// The data is stored in the format:
//
// An 8-byte length prefix, followed by the data entry of that length.
// The length is stored as a 64-bit little-endian encoded uint64.
func (p *NodeStore) RecoverFromWAL() error {
	walPath := filepath.Join(p.dataDir, "wal.log")

	// Check if WAL exists
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		return nil // No WAL, nothing to recover
	}

	// Open WAL for reading
	file, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("RecoverFromWAL: failed to open WAL for recovery: %w", err)
	}
	defer file.Close()

	// Read and apply the data entries from WAL file.
	for {
		// Read the 8 bytelength prefix.
		lenBuf := make([]byte, 8)
		_, err := io.ReadFull(file, lenBuf)
		if err == io.EOF {
			break // End of file
		}
		if err != nil {
			return fmt.Errorf("RecoverFromWAL: failed to read entry length: %w", err)
		}

		// Get prefix length value
		prefixLen := binary.LittleEndian.Uint64(lenBuf)

		// Read actual data entry.
		entryData := make([]byte, prefixLen)
		_, err = io.ReadFull(file, entryData)
		if err != nil {
			return fmt.Errorf("RecoverFromWAL: failed to read entry data: %w", err)
		}

		// Unmarshal the data entry.
		var entry WALEntry
		if err := json.Unmarshal(entryData, &entry); err != nil {
			return fmt.Errorf("RecoverFromWAL: failed to unmarshal WAL entry: %w", err)
		}

		// Apply the data entry.
		switch entry.Operation {
		case "add":
			// Add node to appropriate chunk
			if p.currentChunk == nil || len(p.currentChunk.Nodes) >= p.chunkSize {
				// Create new chunk
				p.currentChunk = &Chunk{
					ID:       p.nextChunkID,
					Nodes:    make(map[uuid.UUID]*Node),
					Modified: true,
				}
				p.chunks[p.nextChunkID] = p.currentChunk
				p.nextChunkID++
			}

			// Add node to chunk
			p.currentChunk.Nodes[entry.NodeID] = entry.Node

			// Update metadata
			metadata := &NodeMetadata{
				ID:       entry.Node.ID,
				Parent:   entry.Node.Parent,
				Children: entry.Node.Children,
			}
			p.nodes[entry.NodeID] = metadata
			p.nameToID[entry.Node.Name] = entry.NodeID

			// Record location
			p.nodeToChunk[entry.NodeID] = ChunkLocation{
				ChunkID: p.currentChunk.ID,
			}

		case "update":
			// Find the chunk containing this node from the nodeToChunk map.
			location, exists := p.nodeToChunk[entry.NodeID]
			if !exists {
				return fmt.Errorf("RecoverFromWAL: node %s not found in chunk mapping during recovery", entry.NodeID)
			}

			// Get or load the chunk
			chunk, exists := p.chunks[location.ChunkID]
			if !exists {
				var loadErr error
				chunk, loadErr = p.loadChunk(location.ChunkID)
				if loadErr != nil {
					// If chunk doesn't exist yet (possible during recovery),
					// create a new one
					chunk = &Chunk{
						ID:       location.ChunkID,
						Nodes:    make(map[uuid.UUID]*Node),
						Modified: true,
					}
					p.chunks[location.ChunkID] = chunk
				}
			}

			// Update node in chunk
			chunk.Nodes[entry.NodeID] = entry.Node
			chunk.Modified = true

			// Update metadata
			metadata, exists := p.nodes[entry.NodeID]
			if !exists {
				uuidMap := make(map[uuid.UUID]struct{})
				metadata = &NodeMetadata{
					ID:       entry.Node.ID,
					Children: make(map[relationship]map[uuid.UUID]struct{}),
				}
				metadata.Children["relationship"] = uuidMap

				p.nodes[entry.NodeID] = metadata
				p.nameToID[entry.Node.Name] = entry.NodeID
			}

			metadata.Parent = entry.Node.Parent
			metadata.Children = entry.Node.Children
		}
	}

	// Flush recovered state
	if err := p.FlushAll(); err != nil {
		return fmt.Errorf("RecoverFromWAL: failed to flush all chunks: %w", err)
	}

	return nil
}

// DebugInfo returns diagnostic information about the store
func (p *NodeStore) DebugInfo() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Count nodes in each chunk
	chunkNodeCounts := make(map[int]int)
	for _, chunk := range p.chunks {
		chunk.mu.RLock()
		chunkNodeCounts[chunk.ID] = len(chunk.Nodes)
		chunk.mu.RUnlock()
	}

	// Count nodes in nodeToChunk by chunk
	nodeToChunkCounts := make(map[int]int)
	for _, loc := range p.nodeToChunk {
		nodeToChunkCounts[loc.ChunkID]++
	}

	// Check for inconsistencies
	inconsistentNodes := make([]string, 0)
	for id, loc := range p.nodeToChunk {
		chunk, exists := p.chunks[loc.ChunkID]
		if !exists {
			inconsistentNodes = append(inconsistentNodes, fmt.Sprintf("Node %s references non-loaded chunk %d", id, loc.ChunkID))
			continue
		}

		chunk.mu.RLock()
		_, exists = chunk.Nodes[id]
		chunk.mu.RUnlock()

		if !exists {
			inconsistentNodes = append(inconsistentNodes, fmt.Sprintf("Node %s not found in referenced chunk %d", id, loc.ChunkID))
		}
	}

	return map[string]interface{}{
		"total_nodes":          len(p.nodes),
		"total_name_mappings":  len(p.nameToID),
		"total_chunk_mappings": len(p.nodeToChunk),
		"loaded_chunks":        len(p.chunks),
		"next_chunk_id":        p.nextChunkID,
		"chunk_size":           p.chunkSize,
		"current_chunk_id":     p.currentChunk.ID,
		"current_chunk_nodes":  len(p.currentChunk.Nodes),
		"chunk_node_counts":    chunkNodeCounts,
		"node_to_chunk_counts": nodeToChunkCounts,
		"inconsistent_nodes":   inconsistentNodes,
	}
}

// DefaultDataDir returns the default data directory for the persistent store
func DefaultDataDir() (string, error) {
	// Get user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("DefaultDataDir: failed to get user home directory: %w", err)
	}

	// Create default data directory path
	dataDir := filepath.Join(homeDir, ".graphed")
	return dataDir, nil
}
