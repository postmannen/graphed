package graphed

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"log"
)

const (
	// Number of root nodes to create
	numRootNodes = 1000
	// Number of subnodes per root node
	numSubNodesPerRoot = 2
	// Number of subsubnodes per subnode
	numSubSubNodesPerSubNode = 1
	// Length of random string values
	randomValueLength = 30
	// Directory for the benchmark
	directory = "./graphed-benchmark"

	chunkSize = 100
)

func BenchmarkAddNodes(b *testing.B) {
	dataDir, createDataDir := createDataDir(b)

	store, err := NewPersistentNodeStore(dataDir, WithChunkSize(chunkSize))
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			b.Fatalf("Failed to close store: %v", err)
		}
	}()

	if createDataDir {
		addBenchmarkNodes(b, store)
	}

	// DEBUG: Open a file to write the node names
	file, err := os.Create(filepath.Join(directory, "node-names.txt"))
	if err != nil {
		b.Fatalf("Failed to create node names file: %v", err)
	}
	defer file.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rnd := rand.Intn(numRootNodes)
		nodeName := fmt.Sprintf("root-%d", rnd)

		nd, err := store.Node(nodeName)
		if err != nil {
			log.Printf("Failed to retrieve node: %v : %v\n", nodeName, err)
		}

		if nd.Name != nodeName {
			log.Printf("Expected  node name to be %v, got '%s'\n", nodeName, nd.Name)
		}

		// // DEBUG: Write the node name to the file
		// _, err = file.WriteString(nodeName + "\n")
		// if err != nil {
		// 	b.Fatalf("Failed to write node name to file: %v", err)
		// }
	}
}

func BenchmarkTraverseNodesDown(b *testing.B) {
	dataDir, createDataDir := createDataDir(b)

	store, err := NewPersistentNodeStore(dataDir, WithChunkSize(chunkSize))
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			b.Fatalf("Failed to close store: %v", err)
		}
	}()

	if createDataDir {
		addBenchmarkNodes(b, store)
	}

	// DEBUG: Open a file to write the node names
	file, err := os.Create(filepath.Join(directory, "node-names.txt"))
	if err != nil {
		b.Fatalf("Failed to create node names file: %v", err)
	}
	defer file.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rnd := rand.Intn(numRootNodes)
		nodeName := fmt.Sprintf("root-%d", rnd)

		nd, err := store.Node(nodeName)
		if err != nil {
			log.Printf("Failed to retrieve node: %v : %v\n", nodeName, err)
		}

		if nd.Name != nodeName {
			log.Printf("Expected  node name to be %v, got '%s'\n", nodeName, nd.Name)
		}

		// DEBUG: Write the node name to the file
		// _, err = file.WriteString(nodeName + " ")
		// if err != nil {
		// 	b.Fatalf("Failed to write node name to file: %v", err)
		// }

		toLookup := nodeName
		ok := false
		for {
			toLookup, ok = benchmarkGetChild(toLookup, store)

			// _, err = file.WriteString(toLookup + " ")
			// if err != nil {
			// 	b.Fatalf("Failed to write node name to file: %v", err)
			// }

			if !ok {
				break
			}
		}

		// _, err = file.WriteString("\n")
		// if err != nil {
		// 	b.Fatalf("Failed to write node name to file: %v", err)
		// }

	}
}

func benchmarkGetChild(nodeName string, store *PersistentNodeStore) (string, bool) {
	nd, err := store.Node(nodeName)
	if err != nil {
		log.Printf("Failed to retrieve node: %v : %v\n", nodeName, err)
	}

	if nd.Name != nodeName {
		log.Printf("Expected  node name to be %v, got '%s'\n", nodeName, nd.Name)
	}

	if len(nd.Children) == 0 {
		return "", false
	}

	// Pick the first child, retrieve it by ID and return the name.
	for childID := range nd.Children {
		n, err := store.NodeByID(childID)
		if err != nil {
			log.Printf("Failed to retrieve node ID: %v : %v\n", nodeName, err)
		}

		return n.Name, true
	}

	return "", false
}

func createDataDir(b *testing.B) (string, bool) {
	// Create a directory for the test. Not using a temp directory so we can reuse the
	// test data for later benchmarks.
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	// defer os.RemoveAll(tempDir) // Clean up when done
	dataDirName := fmt.Sprintf("data-chunck%v-nodes%v-subnodes%v-subsubnodes%v", chunkSize, numRootNodes, numSubNodesPerRoot, numSubSubNodesPerSubNode)
	dataDir := filepath.Join(directory, dataDirName)

	var createDataDir bool
	// Check if the data directory exists, and create it if it doesn't.
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			b.Fatalf("Failed to create data directory: %v", err)
		}
		createDataDir = true
	}

	return dataDir, createDataDir
}

// addBenchmarkNodes adds the benchmark node structure to the store
func addBenchmarkNodes(b *testing.B, store *PersistentNodeStore) {
	// Create a random source
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create root nodes
	for i := 0; i < numRootNodes; i++ {
		rootName := fmt.Sprintf("root-%d", i)
		if err := store.AddNode(rootName, ""); err != nil {
			b.Fatalf("Failed to add root node %s: %v", rootName, err)
		}

		// Create subnodes for each root node
		for j := 0; j < numSubNodesPerRoot; j++ {
			subNodeName := fmt.Sprintf("%s-sub-%d", rootName, j)
			if err := store.AddNode(subNodeName, rootName); err != nil {
				b.Fatalf("Failed to add subnode %s: %v", subNodeName, err)
			}

			// Create subsubnodes for each subnode
			for k := 0; k < numSubSubNodesPerSubNode; k++ {
				subSubNodeName := fmt.Sprintf("%s-subsub-%d", subNodeName, k)
				if err := store.AddNode(subSubNodeName, subNodeName); err != nil {
					b.Fatalf("Failed to add subsubnode %s: %v", subSubNodeName, err)
				}

				// Add random value to subsubnode - use the random source directly
				randomBytes := make([]byte, randomValueLength)
				for i := range randomBytes {
					randomBytes[i] = byte(r.Intn(256))
				}
				if err := store.AddToValues(subSubNodeName, randomBytes); err != nil {
					b.Fatalf("Failed to add value to subsubnode %s: %v", subSubNodeName, err)
				}
			}
		}
	}
}

// BenchmarkComplexNodeRetrieval benchmarks the retrieval of nodes from a complex structure
func BenchmarkComplexNodeRetrieval(b *testing.B) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "graphed-benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up when done

	// Create data directory
	dataDir := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		b.Fatalf("Failed to create data directory: %v", err)
	}

	b.StopTimer()
	// Create a store and populate it with benchmark nodes
	store, err := NewPersistentNodeStore(dataDir, WithChunkSize(100))
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create the nodes
	addBenchmarkNodes(b, store)

	// Create a random source for the benchmark
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Reset timer before the actual benchmark starts
	b.ResetTimer()
	b.StartTimer()

	// Run the benchmark multiple times
	for i := 0; i < b.N; i++ {
		// Select a random root node
		rootIdx := r.Intn(numRootNodes)
		rootName := fmt.Sprintf("root-%d", rootIdx)

		// Retrieve the root node
		_, err := store.Node(rootName)
		if err != nil {
			b.Fatalf("Failed to retrieve root node %s: %v", rootName, err)
		}

		// Select a random subnode
		subIdx := r.Intn(numSubNodesPerRoot)
		subNodeName := fmt.Sprintf("%s-sub-%d", rootName, subIdx)

		// Retrieve the subnode
		_, err = store.Node(subNodeName)
		if err != nil {
			b.Fatalf("Failed to retrieve subnode %s: %v", subNodeName, err)
		}

		// Select a random subsubnode
		subSubIdx := r.Intn(numSubSubNodesPerSubNode)
		subSubNodeName := fmt.Sprintf("%s-subsub-%d", subNodeName, subSubIdx)

		// Retrieve the subsubnode
		subSubNode, err := store.Node(subSubNodeName)
		if err != nil {
			b.Fatalf("Failed to retrieve subsubnode %s: %v", subSubNodeName, err)
		}

		// Verify the value exists
		if len(subSubNode.Values) == 0 {
			b.Fatalf("Subsubnode %s has no values", subSubNodeName)
		}
	}
}

func IsEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}
