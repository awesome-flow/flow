package util

import (
	"booking/msgrelay/flow"
	"reflect"
	"testing"
)

type A struct {
	*flow.Connector
}

func NewA() *A {
	return &A{flow.NewConnector()}
}

func TestNTree_buildCompTree(t *testing.T) {
	// ppl := map[string]config.CfgBlockPipeline{
	// 	"A": config.CfgBlockPipeline{Connect: "B", Links: []string{"C", "D"}},
	// 	"B": config.CfgBlockPipeline{Routes: map[string]string{"e": "E", "f": "F"}},
	// }

	a, b, c, d, e, f := NewA(), NewA(), NewA(), NewA(), NewA(), NewA()

	tree := &NTree{}
	aNode := tree.FindOrInsert(a)
	aNode.FindOrInsert(c)
	aNode.FindOrInsert(d)

	bNode := aNode.FindOrInsert(b)
	bNode.FindOrInsert(e)
	bNode.FindOrInsert(f)

	if ptr := tree.Find(a); ptr != nil {
		if ptr.value != a {
			t.Errorf("Unexpected contents in ptr: %+v", ptr)
		}
	} else {
		t.Errorf("Failed to find a")
	}
	nodeA, nodeInsA := tree.Find(a), tree.FindOrInsert(a)
	if nodeA != nodeInsA {
		t.Errorf("Find and findAndInsert returned different values")
	}
	nodeA, parentNodeB := tree.Find(a), tree.FindParent(b)
	if nodeA != parentNodeB {
		t.Errorf("Expected node A to be the parent of node B")
	}
	parentNodeC, parentNodeD, nodeA := tree.FindParent(c), tree.FindParent(d), tree.Find(a)
	if nodeA != parentNodeC || nodeA != parentNodeD {
		t.Errorf("Expected node A to be the parent of nodes C and D")
	}
	nodeB, parentNodeE, parentNodeF := tree.Find(b), tree.FindParent(e), tree.FindParent(f)
	if nodeB != parentNodeE || nodeB != parentNodeF {
		t.Errorf("Expected node B to be the parent of nodes E and F")
	}
}

func TestNTRee_FindCommonParent(t *testing.T) {
	//      R
	//   A      B
	// C  D   E   F
	//G    H        K
	tree := &NTree{}
	nodeA := tree.FindOrInsert("A")
	nodeB := tree.FindOrInsert("B")
	nodeC := nodeA.FindOrInsert("C")
	nodeD := nodeA.FindOrInsert("D")
	nodeF := nodeB.FindOrInsert("F")
	nodeB.FindOrInsert("E")
	nodeC.FindOrInsert("G")
	nodeD.FindOrInsert("H")
	nodeF.FindOrInsert("K")

	tests := []struct {
		name   string
		lookup []interface{}
		expLCA *NTree
	}{
		{"A and B", []interface{}{"A", "B"}, tree},
		{"C and D", []interface{}{"C", "D"}, nodeA},
		{"E and F", []interface{}{"E", "F"}, nodeB},
		{"G and H", []interface{}{"G", "H"}, nodeA},
		{"E and K", []interface{}{"E", "K"}, nodeB},
		{"G and E", []interface{}{"G", "E"}, tree},
		{"H and E", []interface{}{"H", "E"}, tree},
		{"H and A", []interface{}{"H", "A"}, nodeA},
		{"E and B", []interface{}{"E", "B"}, nodeB},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			lca := tree.FindCommonParent(testCase.lookup...)
			if lca != testCase.expLCA {
				t.Errorf("Unexpected LCA in %s: expected: %+v, got: %+v",
					testCase.name, testCase.expLCA, lca)
			}
		})
	}
}

func TestNTree_DetachNonEmpty(t *testing.T) {
	tree := &NTree{}
	nodeA := tree.FindOrInsert("A")
	tree.FindOrInsert("B")
	tree.FindOrInsert("C")
	detA := tree.Detach("A")
	if detA != nodeA {
		t.Errorf("Unexpected value returned by Detach: %+v, expected: %+v", detA, nodeA)
	}
	lookupA := tree.Find("A")
	if lookupA != nil {
		t.Errorf("Did not expect to find node in the tree once detached")
	}
}

func TestNTree_DetachEmpty(t *testing.T) {
	tree := &NTree{}
	tree.FindOrInsert("A")
	tree.FindOrInsert("B")
	tree.FindOrInsert("C")
	detD := tree.Detach("D")
	if detD != nil {
		t.Errorf("Unexpected value returned by Detach: %+v, expected: nil", detD)
	}
}

func TestNTree_PostTraversal(t *testing.T) {
	//      R
	//   A      B
	// C  D   E   F
	//G    H        K
	tree := &NTree{}
	nodeA := tree.FindOrInsert("A")
	nodeB := tree.FindOrInsert("B")
	nodeC := nodeA.FindOrInsert("C")
	nodeD := nodeA.FindOrInsert("D")
	nodeB.FindOrInsert("E")
	nodeF := nodeB.FindOrInsert("F")
	nodeC.FindOrInsert("G")
	nodeD.FindOrInsert("H")
	nodeF.FindOrInsert("K")
	res := tree.PostTraversal()
	if !reflect.DeepEqual(res, []interface{}{
		"G", "C", "H", "D", "A", "E", "K", "F", "B",
	}) {
		t.Errorf("Unexpected tree post-traversal result: %+v", res)
	}
}

func TestNTree_PreTraversal(t *testing.T) {
	//      R
	//   A      B
	// C  D   E   F
	//G    H        K
	tree := &NTree{}
	nodeA := tree.FindOrInsert("A")
	nodeB := tree.FindOrInsert("B")
	nodeC := nodeA.FindOrInsert("C")
	nodeD := nodeA.FindOrInsert("D")
	nodeB.FindOrInsert("E")
	nodeF := nodeB.FindOrInsert("F")
	nodeC.FindOrInsert("G")
	nodeD.FindOrInsert("H")
	nodeF.FindOrInsert("K")
	res := tree.PreTraversal()
	if !reflect.DeepEqual(res, []interface{}{
		"A", "C", "G", "D", "H", "B", "E", "F", "K",
	}) {
		t.Errorf("Unexpected tree pre-traversal result: %+v", res)
	}
}
