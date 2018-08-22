package data

import (
	"fmt"
)

type TopologyNode interface {
	GetName() string
}

type edge struct {
	from string
	to   string
}

type Topology struct {
	nodes []TopologyNode
	edges []*edge
}

func NewTopology(extNodes ...TopologyNode) *Topology {
	return &Topology{
		edges: make([]*edge, 0),
		nodes: extNodes,
	}
}

func (top *Topology) Connect(from string, to string) {
	top.edges = append(top.edges, &edge{from, to})
}

func (top *Topology) Sort() ([]TopologyNode, error) {
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
	lenNodes := len(top.nodes)
	adjMx := make([][]uint16, lenNodes+1)
	nameToIx := make(map[string]int)
	res := make([]TopologyNode, 0)
	ixSet := make(map[int]bool)
	emptyRes := []TopologyNode{}

	for ix := 0; ix <= lenNodes; ix++ {
		adjMx[ix] = make([]uint16, lenNodes)
		if ix < lenNodes {
			nameToIx[top.nodes[ix].GetName()] = ix
		}
	}

	for _, edge := range top.edges {
		fromIx, ok := nameToIx[edge.from]
		if !ok {
			return emptyRes, fmt.Errorf("Unknown node connecting from: %s", edge.from)
		}
		toIx, ok := nameToIx[edge.to]
		if !ok {
			return emptyRes, fmt.Errorf("Unknown node connecting to: %s", edge.to)
		}
		adjMx[fromIx][toIx] = 1
		adjMx[lenNodes][toIx]++
	}
	for ix, v := range adjMx[lenNodes] {
		if v == 0 {
			ixSet[ix] = true
		}
	}

	L := make([]int, 0)
	visited := make(map[int]bool)
	for len(ixSet) > 0 {
		var fromIx int
		for ix := range ixSet {
			fromIx = ix
			break
		}
		if _, ok := visited[fromIx]; ok {
			return emptyRes, fmt.Errorf("Cycle detected on node %s",
				top.nodes[fromIx].GetName())
		}
		delete(ixSet, fromIx)
		L = append(L, fromIx)
		visited[fromIx] = true
		for toIx := 0; toIx < lenNodes; toIx++ {
			if adjMx[fromIx][toIx] > 0 {
				adjMx[fromIx][toIx] = 0
				adjMx[lenNodes][toIx]--
				if adjMx[lenNodes][toIx] == 0 {
					ixSet[toIx] = true
				}
			}
		}
	}
	for ix, cnt := range adjMx[lenNodes] {
		if cnt > 0 {
			return make([]TopologyNode, 0),
				fmt.Errorf("Node %s contains unresolved edges",
					top.nodes[ix].GetName())
		}
	}
	for _, ix := range L {
		res = append(res, top.nodes[ix])
	}

	return res, nil
}
