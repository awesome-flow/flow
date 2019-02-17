package data

import (
	"fmt"
)

type TopologyNode interface{}

type TopologyEdge struct {
	From TopologyNode
	To   TopologyNode
}

type Topology struct {
	Edges map[TopologyEdge]struct{}
	Nodes map[TopologyNode]struct{}
}

func NewTopology(nodes ...TopologyNode) *Topology {
	nodeset := make(map[TopologyNode]struct{})
	for _, node := range nodes {
		nodeset[node] = struct{}{}
	}
	return &Topology{
		Edges: make(map[TopologyEdge]struct{}),
		Nodes: nodeset,
	}
}

func (top *Topology) AddNode(node TopologyNode) {
	top.Nodes[node] = struct{}{}
}

// ConnectTo creates a directed edge between node "from" to node "to".
// If a topology represents a dependency graph, this notation should be
// interpreted as: node "from" depends on node "to", i.e. in a case of a
// topological sort node "to" would be visited before node "from".
//
// Example
//
// top := NewTopology(A, B)
// top.Connect(A, B) // A -> B
// top.Sort() // returns {B, A}: B has been visited, which satisfies A
// dependencies.
func (top *Topology) Connect(from, to TopologyNode) error {
	if _, ok := top.Nodes[from]; !ok {
		return fmt.Errorf("Can not connect from unknown node: %#v", from)
	}
	if _, ok := top.Nodes[to]; !ok {
		return fmt.Errorf("Can not connect to unknown node: %#v", to)
	}
	top.Edges[TopologyEdge{From: from, To: to}] = struct{}{}

	return nil
}

func (top *Topology) Sort() ([]TopologyNode, error) {
	temp := make(map[TopologyNode]bool)
	perm := make(map[TopologyNode]bool)
	outs := make(map[TopologyNode][]TopologyNode)
	for edge := range top.Edges {
		if _, ok := outs[edge.From]; !ok {
			outs[edge.From] = make([]TopologyNode, 0, 1)
		}
		outs[edge.From] = append(outs[edge.From], edge.To)
	}

	var visitAll func([]TopologyNode) ([]TopologyNode, error)
	visitAll = func(nodes []TopologyNode) ([]TopologyNode, error) {
		res := make([]TopologyNode, 0)
		for _, node := range nodes {
			if perm[node] {
				continue
			}
			if temp[node] {
				return nil, fmt.Errorf("Detected graph cycle on node %#v", node)
			}
			temp[node] = true
			if subs, ok := outs[node]; ok {
				subsorted, err := visitAll(subs)
				if err != nil {
					return nil, err
				}
				res = append(res, subsorted...)
			}
			perm[node] = true
			res = append(res, node)
		}
		return res, nil
	}
	nodes := make([]TopologyNode, 0, len(top.Nodes))
	for node := range top.Nodes {
		nodes = append(nodes, node)
	}

	if res, err := visitAll(nodes); err != nil {
		return nil, err
	} else {
		return res, nil
	}
}
