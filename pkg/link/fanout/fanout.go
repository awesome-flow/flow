package link

import (
	"fmt"
	"sync"

	"github.com/awesome-flow/flow/pkg/core"
)

type RingLink struct {
	self core.Link
	next *RingLink
	prev *RingLink
}

type Fanout struct {
	Name     string
	ringHead *RingLink
	*sync.RWMutex
	*core.Connector
}

func New(name string, params core.Params, context *core.Context) (core.Link, error) {
	ft := &Fanout{
		name,
		nil,
		&sync.RWMutex{},
		core.NewConnector(),
	}
	for _, ch := range ft.GetMsgCh() {
		go func(ch chan *core.Message) {
			ft.fanout(ch)
		}(ch)
	}
	return ft, nil
}

func (ft *Fanout) ConnectTo(core.Link) error {
	panic("Fanout is not supposed to be connected")
}

func (ft *Fanout) fanout(ch chan *core.Message) {
	for msg := range ch {
		ft.RLock()
		head := ft.ringHead
		if head == nil {
			msg.AckFailed()
			ft.RUnlock()
			continue
		}
		head.self.Recv(msg)
		ft.ringHead = head.next
		ft.RUnlock()
	}
}

func (ft *Fanout) LinkTo(links []core.Link) error {
	for _, link := range links {
		ft.AddLink(link)
	}
	return nil
}

func (ft *Fanout) AddLink(link core.Link) error {
	return ft.addRingLink(&RingLink{self: link})
}

func (ft *Fanout) FindLink(link core.Link) (*RingLink, bool) {
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

func (ft Fanout) RemoveLink(link core.Link) error {
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
	ft.Lock()
	defer ft.Unlock()
	if ft.ringHead == nil {
		return 0
	}
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
