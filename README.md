# graphed

Graph database written in Go .... or rather a POC for a graph database written in Go :)

## Features

### In-Memory and Persistent Storage

Graphed supports both in-memory and persistent storage options:

- **In-Memory Storage**: Fast, but data is lost when the program exits
- **Persistent Storage**: Stores data on disk for durability and recovery

The persistent storage implementation includes:

- Chunked storage for efficient I/O
- Write-ahead logging (WAL) for crash recovery
- In-memory indexes for fast lookups
- Automatic chunk management

## Usage

### In-Memory Storage

```go
// Create a new in-memory store
store := graphed.NewNodeStore()

// Add nodes
store.AddNode("root", "")
store.AddNode("child", "root")

// Add values
store.AddToValues("child", []byte("some data"))

// Retrieve nodes
node, err := store.Node("child")
if err != nil {
    // Handle error
}
```

### Persistent Storage

```go
// Create a new persistent store
dataDir := "/path/to/data"
store, err := graphed.NewNodeStoreAdapter(dataDir)
if err != nil {
    // Handle error
}
defer store.Close() // Important: Close to ensure data is flushed

// Use the same API as in-memory store
store.AddNode("root", "")
store.AddNode("child", "root")
store.AddToValues("child", []byte("persistent data"))

// Data will be saved to disk automatically
```

### Configuration Options

```go
// Custom chunk size (default is 100 nodes per chunk)
store, err := graphed.NewNodeStoreAdapter(dataDir, graphed.WithChunkSize(500))
```

## TODO/IDEAS

### Traverse graph in any direction ?

### HASH/Checksum of node, tamper protection ?

Not sure if this makes sense, but a thing to check out further.

### Built-in version control of node data ?

### Query language ?

### Storage Optimizations, Compression ?

### Support for different storage backends, disk and memory

Implemented, but should be refactored to to use the same function, and rather choose to have a storage interface as an argument.
