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
	Edges []TopologyEdge
	Nodes []TopologyNode
}

func NewTopology(nodes ...TopologyNode) *Topology {
	return &Topology{
		Edges: make([]TopologyEdge, 0),
		Nodes: nodes,
	}
}

func (top *Topology) Connect(from, to TopologyNode) {
	top.Edges = append(top.Edges, TopologyEdge{From: from, To: to})
}

func (top *Topology) Sort() ([]TopologyNode, error) {
	temp := make(map[TopologyNode]bool)
	perm := make(map[TopologyNode]bool)
	outs := make(map[TopologyNode][]TopologyNode)
	for _, edge := range top.Edges {
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

	if res, err := visitAll(top.Nodes); err != nil {
		return nil, err
	} else {
		return res, nil
	}
}
