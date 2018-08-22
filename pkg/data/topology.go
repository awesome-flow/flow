package data

import (
	"fmt"
)

type TopologyNode interface {
	GetName() string
	ConnectsTo() []string
}

type node struct {
	ix       int
	origNode TopologyNode
}

type Topology struct {
	nodes []*node
	adjMx [][]uint16
}

func NewTopology(extNodes ...TopologyNode) (*Topology, error) {
	// Adjacency matrix has an extra field indicating the total
	// number of edges to this node (S)
	// Semantics: row index is node FROM, col index is node TO.
	// Example:
	//   0 1 2 3
	// 0 X 1 0 1
	// 1 0 X 1 0
	// 2 0 0 X 0
	// 3 1 0 1 X
	//
	// S 1 1 2 1
	//
	// Node 1 is connected to node 2, so does node 3. Total
	// number of incoming edges to node 2 is also 2.
	//
	lenNodes := len(extNodes)
	adjMx := make([][]uint16, lenNodes+1)
	nodes := make([]*node, lenNodes)
	nameToIx := make(map[string]int)
	for ix := 0; ix <= lenNodes; ix++ {
		adjMx[ix] = make([]uint16, lenNodes)
		if ix < lenNodes {
			nodes[ix] = &node{ix: ix, origNode: extNodes[ix]}
			nameToIx[extNodes[ix].GetName()] = ix
		}
	}
	for fromIx, extNode := range extNodes {
		for _, name := range extNode.ConnectsTo() {
			toIx, ok := nameToIx[name]
			if !ok {
				return nil, fmt.Errorf("Node with name '%s' could not be found, "+
					"requested by node: %s", name, extNode.GetName())
			}
			adjMx[fromIx][toIx] = 1
			adjMx[lenNodes][toIx]++
		}
	}
	return &Topology{
		nodes: nodes,
		adjMx: adjMx,
	}, nil
}

func (top *Topology) Sort() ([]TopologyNode, error) {
	// TODO
	fmt.Printf("adj mx: %+v", top.adjMx)
	return make([]TopologyNode, 0), nil
}
