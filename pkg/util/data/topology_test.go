package data

import (
	"reflect"
	"strings"
	"testing"
)

type testNode struct {
	name string
}

func (tn *testNode) GetName() string {
	return tn.name
}

func newNode(name string) *testNode {
	return &testNode{name}
}

func TestTopology_SortEmpty(t *testing.T) {
	nodes := []TopologyNode{}
	top := NewTopology(nodes...)
	sorted, err := top.Sort()
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if l := len(sorted); l != 0 {
		t.Errorf("Unexpected length of sorted arr: %d (want 0)", l)
	}
}

func TestTopology_SortSingle(t *testing.T) {
	nodes := []TopologyNode{
		newNode("1"),
	}
	top := NewTopology(nodes...)
	sorted, err := top.Sort()
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if l := len(sorted); l != 1 {
		t.Errorf("Unexpected length of sorted arr: %d (want 0)", l)
	}
	if !reflect.DeepEqual(sorted[0], nodes[0]) {
		t.Errorf("Unexpected element in sorted list: %+v", sorted[0])
	}
}

type StringerNode string

func TestTopology_SortUnresolvable(t *testing.T) {
	nodes := []TopologyNode{
		StringerNode("1"),
		StringerNode("2"),
		StringerNode("3"),
	}

	top := NewTopology(nodes...)
	top.Connect(nodes[0], nodes[1])
	top.Connect(nodes[1], nodes[2])
	top.Connect(nodes[2], nodes[0])

	sorted, err := top.Sort()
	if err == nil {
		t.Errorf("Expected an error from a cycled graph")
	}
	if l := len(sorted); l != 0 {
		t.Errorf("Unexpected length of the sorted result: %d (want 0)", l)
	}
	res := make([]string, 0, len(sorted))
	for _, node := range sorted {
		res = append(res, string(node.(StringerNode)))
	}
	t.Log(strings.Join(res, " -> "))
}

/*
https://upload.wikimedia.org/wikipedia/commons/thumb/0/03/Directed_acyclic_graph_2.svg/1280px-Directed_acyclic_graph_2.svg.png

   (5)  (7) (3)
    |  / |  /
    v /  v /
   (11) (8)
	| \ \|
	v  \ v \
   (2)  (9) (10)

*/
func TestTopology_SortExample(t *testing.T) {
	node2, node3, node5, node7, node8, node9, node10, node11 :=
		StringerNode("2"),
		StringerNode("3"),
		StringerNode("5"),
		StringerNode("7"),
		StringerNode("8"),
		StringerNode("9"),
		StringerNode("10"),
		StringerNode("11")

	connections := map[StringerNode][]StringerNode{
		node5:  {node11},
		node7:  {node11, node8},
		node3:  {node8, node10},
		node11: {node2, node9, node10},
		node8:  {node9},
	}

	top := NewTopology(node2, node3, node5, node7, node8, node9, node10, node11)

	for from, tos := range connections {
		for _, to := range tos {
			if err := top.Connect(from, to); err != nil {
				t.Fatalf(err.Error())
			}
		}
	}

	sorted, err := top.Sort()
	if err != nil {
		t.Fatalf("Failed to perform topological sort of the graph: %s", err)
	}

	visited := make(map[StringerNode]bool)
	for _, node := range sorted {
		if deps, ok := connections[node.(StringerNode)]; ok {
			for _, dep := range deps {
				if _, ok := visited[dep]; !ok {
					t.Fatalf(
						"Node %#v was expected to be visited after %#v. Full order: %#v\n",
						node,
						dep,
						sorted)
				}
			}
		}
		visited[node.(StringerNode)] = true
	}
}
