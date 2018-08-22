package data

import "testing"

type testNode struct {
	name     string
	connects []string
}

func (tn *testNode) GetName() string {
	return tn.name
}

func (tn *testNode) ConnectsTo() []string {
	return tn.connects
}

func newNode(name string, connects []string) *testNode {
	return &testNode{name, connects}
}

func TestTopology_Sort(t *testing.T) {
	nodes := []TopologyNode{
		newNode("5", []string{"11"}),
		newNode("7", []string{"11", "8"}),
		newNode("3", []string{"8", "10"}),
		newNode("11", []string{"2", "9", "10"}),
		newNode("8", []string{"9"}),
		newNode("2", []string{}),
		newNode("9", []string{}),
		newNode("10", []string{}),
	}
	top, err := NewTopology(nodes...)
	if err != nil {
		t.Errorf("Failed to build a new topology: %s", err)
	}
	sorted, err := top.Sort()
	if err != nil {
		t.Errorf("Failed to sort the topology: %s", err)
	}
	if len(sorted) != len(nodes) {
		t.Errorf("Unexpected length of the sorted list: %d (want %d).\n"+
			"List contents: %+v", len(sorted), len(nodes), sorted)
	}
}
