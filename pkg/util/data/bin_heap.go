package data

import (
	"sync"
)

type BinHeap struct {
	mutex *sync.Mutex
	vals  []*BinHeapNode
}

type BinHeapNode struct {
	weight uint32
	value  interface{}
}

func NewBinHeap() *BinHeap {
	return &BinHeap{
		mutex: &sync.Mutex{},
		vals:  make([]*BinHeapNode, 0),
	}
}

func (hp *BinHeap) Insert(weight uint32, val interface{}) {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()
	hp.vals = append(hp.vals, &BinHeapNode{weight: weight, value: val})
	hp.heapify()
}

func (hp *BinHeap) heapify() {
	l := len(hp.vals) - 1
	if l <= 0 {
		return
	}
	for {
		p := (l - 1) / 2
		if hp.vals[l].weight >= hp.vals[p].weight {
			// swap them
			hp.vals[p], hp.vals[l] = hp.vals[l], hp.vals[p]
		} else {
			break
		}
		if p <= 0 {
			break
		}
	}
}

func (hp *BinHeap) GetMax() interface{} {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()
	if len(hp.vals) > 0 {
		return hp.vals[0].value
	}
	return nil
}
