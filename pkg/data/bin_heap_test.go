package data

import (
	"reflect"
	"testing"
)

func TestBinHeap_Insert(t *testing.T) {
	tests := []struct {
		name     string
		elements map[string]uint32
		expected [][]*BinHeapNode
	}{
		{
			name: "Single node",
			elements: map[string]uint32{
				"A": 1,
			},
			expected: [][]*BinHeapNode{
				[]*BinHeapNode{
					&BinHeapNode{value: "A", weight: 1},
				},
			},
		},
		{
			name: "2 nodes",
			elements: map[string]uint32{
				"A": 1, "B": 2,
			},
			expected: [][]*BinHeapNode{
				[]*BinHeapNode{
					&BinHeapNode{value: "B", weight: 2},
					&BinHeapNode{value: "A", weight: 1},
				},
			},
		},
		{
			name: "3 nodes, 2 with equal weights",
			elements: map[string]uint32{
				"A": 1, "B": 2, "C": 1,
			},
			expected: [][]*BinHeapNode{
				[]*BinHeapNode{
					&BinHeapNode{value: "B", weight: 2},
					&BinHeapNode{value: "A", weight: 1},
					&BinHeapNode{value: "C", weight: 1},
				},
				[]*BinHeapNode{
					&BinHeapNode{value: "B", weight: 2},
					&BinHeapNode{value: "C", weight: 1},
					&BinHeapNode{value: "A", weight: 1},
				},
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			bh := NewBinHeap()
			for v, p := range testCase.elements {
				bh.Insert(p, v)
			}
			matchFound := false
			for _, exp := range testCase.expected {
				if reflect.DeepEqual(bh.vals, exp) {
					matchFound = true
					break
				}
			}
			if !matchFound {
				t.Errorf("Unexpected contents of the tree: got %+v, expected: %+v",
					bh.vals, testCase.expected)
			}
		})
	}
}
