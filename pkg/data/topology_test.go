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

func TestTopology_SortUnresolvable(t *testing.T) {
	nodes := []TopologyNode{
		newNode("1"),
		newNode("2"),
		newNode("3"),
	}
	top := NewTopology(nodes...)
	top.Connect("1", "2")
	top.Connect("2", "3")
	top.Connect("3", "1")
	sorted, err := top.Sort()
	if err == nil {
		t.Errorf("Expected an error from a cycled graph")
	}
	if l := len(sorted); l != 0 {
		t.Errorf("Unexpected length of the sorted result: %d (want 0)", l)
	}
	res := make([]string, len(sorted))
	for ix, node := range sorted {
		res[ix] = node.GetName()
	}
	t.Log(strings.Join(res, " -> "))
}

func TestTopology_Sort(t *testing.T) {
	nodes := []TopologyNode{
		newNode("5"),
		newNode("7"),
		newNode("3"),
		newNode("11"),
		newNode("8"),
		newNode("2"),
		newNode("9"),
		newNode("10"),
	}
	connectsTo := map[string][]string{
		"5":  []string{"11"},
		"7":  []string{"11", "8"},
		"3":  []string{"8", "10"},
		"11": []string{"2", "9", "10"},
		"8":  []string{"9"},
	}
	deps := make(map[string][]string)
	top := NewTopology(nodes...)
	for from, tos := range connectsTo {
		for _, to := range tos {
			top.Connect(from, to)
			if _, ok := deps[to]; !ok {
				deps[to] = make([]string, 0)
			}
			deps[to] = append(deps[to], from)
		}
	}
	sorted, err := top.Sort()
	if err != nil {
		t.Errorf("Failed to sort the topology: %s", err)
	}
	if len(sorted) != len(nodes) {
		t.Errorf("Unexpected length of the sorted list: %d (want %d).\n"+
			"List contents: %+v", len(sorted), len(nodes), sorted)
	}
	resolved := make(map[string]bool)

	res := make([]string, len(nodes))
	for ix, node := range sorted {
		if node.GetName() == "" {
			t.Errorf("Unnamed node, most probably an empty node is returned")
		}
		res[ix] = node.GetName()
		resolved[node.GetName()] = true
		ds, ok := deps[node.GetName()]
		if !ok {
			continue
		}
		for _, d := range ds {
			if _, ok := resolved[d]; !ok {
				t.Errorf("Node %s has been resolved before node %s",
					node.GetName(), d)
			}
		}
	}
}
