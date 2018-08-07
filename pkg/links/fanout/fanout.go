package links

import (
	"booking/msgrelay/flow"
	"fmt"
	"sync"
)

type RingLink struct {
	self flow.Link
	next *RingLink
	prev *RingLink
}

type Fanout struct {
	Name     string
	ringHead *RingLink
	*sync.Mutex
	*flow.Connector
}

func NewFanout(name string, params flow.Params) (flow.Link, error) {
	ft := &Fanout{
		name,
		nil,
		&sync.Mutex{},
		flow.NewConnector(),
	}
	go ft.fanout()
	return ft, nil
}

func (ft *Fanout) ConnectTo(flow.Link) error {
	panic("Fanout is not supposed to be connected")
}

func (ft *Fanout) fanout() {
	for msg := range ft.GetMsgCh() {
		head := ft.ringHead
		if head == nil {
			msg.AckFailed()
			continue
		}
		head.self.Recv(msg)
		ft.ringHead = head.next
	}
}

func (ft *Fanout) LinkTo(links []flow.Link) error {

	for _, link := range links {
		ft.AddLink(link)
	}

	return nil
}

func (ft *Fanout) AddLink(link flow.Link) error {
	return ft.addRingLink(&RingLink{self: link})
}

func (ft *Fanout) FindLink(link flow.Link) (*RingLink, bool) {
	ft.Lock()
	defer ft.Unlock()

	if link == nil {
		return nil, false
	}

	if ft.ringHead == nil {
		return nil, false
	}

	ptr := ft.ringHead
	found := false
	for {
		if ptr.self == link {
			found = true
			break
		}
		ptr = ptr.next
		if ptr == ft.ringHead {
			break
		}
	}

	if !found {
		return nil, false
	}

	return ptr, true
}

func (ft *Fanout) addRingLink(rl *RingLink) error {
	ft.Lock()
	defer ft.Unlock()
	head := ft.ringHead
	if head == nil {
		rl.next, rl.prev = rl, rl
	} else {
		rl.next, rl.prev = head.next, head
		head.next.prev, head.next = rl, rl
	}
	ft.ringHead = rl

	return nil
}

func (ft Fanout) RemoveLink(link flow.Link) error {
	if ptr, ok := ft.FindLink(link); ok {
		return ft.removeRingLink(ptr)
	} else {
		return fmt.Errorf("Link could not be found in the ring")
	}
}

func (ft *Fanout) removeRingLink(rl *RingLink) error {
	ft.Lock()
	defer ft.Unlock()
	if rl == nil {
		return fmt.Errorf("RingLink is empty")
	}
	next := rl.next
	if rl != next {
		rl.prev.next = rl.next
		rl.next.prev = rl.prev
		if rl == ft.ringHead {
			ft.ringHead = next
		}
	} else {
		ft.ringHead = nil
	}

	rl.next = nil
	rl.prev = nil

	return nil
}

func (ft *Fanout) RingSize() int {
	if ft.ringHead == nil {
		return 0
	}
	ft.Lock()
	defer ft.Unlock()
	cnt := 1
	head := ft.ringHead
	ptr := head.next
	for {
		if ptr == head {
			break
		}
		cnt++
		ptr = ptr.next
	}
	return cnt
}
