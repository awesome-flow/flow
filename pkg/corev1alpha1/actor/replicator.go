package actor

import (
	"fmt"
	"math/bits"
	"math/rand"
	"sync"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	MaxPeersCnt = 64
	ReplTimeout = 50 * time.Millisecond
)

type ReplicateMode uint8

const (
	ReplicateUnknown ReplicateMode = iota
	ReplicateFanout
	ReplicateRand
	ReplicateNcopy
	ReplicateAll
)

type Replicator struct {
	name      string
	ctx       *core.Context
	maskfunc  func(uint64, int) uint64
	queueIn   chan *core.Message
	queuesOut []chan *core.Message
	lock      sync.Mutex
	wg        sync.WaitGroup
}

var _ core.Actor = (*Replicator)(nil)

func NewReplicator(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	mode, ok := params["mode"]
	if !ok {
		return nil, fmt.Errorf("replicator %s is missing `mode` config", name)
	}

	r := &Replicator{
		name:      name,
		ctx:       ctx,
		queueIn:   make(chan *core.Message),
		queuesOut: make([]chan *core.Message, 0),
	}

	var maskfunc func(uint64, int) uint64
	switch mode.(string) {
	case "fanout":
		maskfunc = maskFanout
	case "rand":
		maskfunc = maskRand
	case "ncopy":
		maskfunc = maskNcopy
	case "all":
		maskfunc = maskAll
	default:
		return nil, fmt.Errorf("replicator %s `mode` is unknown: %s", name, mode.(string))
	}

	r.maskfunc = maskfunc

	return r, nil
}

func maskFanout(mask uint64, lenq int) uint64 {
	if lenq == 0 {
		return 0
	}
	bshift := uint64((1 << uint64(lenq)) - 1)
	mask &= bshift
	mask = ((mask << 1) | (mask >> (uint64(lenq) - 1))) & bshift
	if mask == 0 {
		mask = 1
	}
	return mask
}

func maskRand(mask uint64, lenq int) uint64 {
	return 1 << uint64(rand.Int63n(int64(lenq)))
}

func maskNcopy(uint64, int) uint64 {
	panic("not implemented")
}

func maskAll(mask uint64, lenq int) uint64 {
	return (1 << uint64(lenq)) - 1
}

func (r *Replicator) replicate(msg *core.Message, mask uint64) error {
	wg := sync.WaitGroup{}
	ix := 0
	cnt := bits.OnesCount64(mask)
	res := make(chan core.MsgStatus, cnt)
	defer close(res)
	for mask > 0 {
		if mask&0x1 == 1 {
			wg.Add(1)
			go func(ix int) {
				msgcp := msg.Copy()
				r.queuesOut[ix] <- msgcp
				select {
				case s := <-msgcp.AwaitChan():
					res <- s
				case <-time.After(ReplTimeout):
					msg.Complete(core.MsgStatusTimedOut)
					res <- core.MsgStatusTimedOut
				}
				wg.Done()
			}(ix)
		}
		ix++
		mask >>= 1
	}
	wg.Wait()
	var compsts uint8
	var s core.MsgStatus
	for i := 0; i < cnt; i++ {
		s = <-res
		switch s {
		case core.MsgStatusDone:
			compsts |= 1 << 0
		case core.MsgStatusPartialSend:
			compsts |= 1 << 1
		case core.MsgStatusTimedOut:
			compsts |= 1 << 2
		default:
			compsts |= 1 << 3
		}
	}
	if compsts == 1 {
		msg.Complete(core.MsgStatusDone)
	} else if compsts>>1 == 1 {
		msg.Complete(core.MsgStatusPartialSend)
	} else if compsts>>2 == 1 {
		msg.Complete(core.MsgStatusTimedOut)
	} else {
		msg.Complete(core.MsgStatusFailed)
	}

	return nil
}

func (r *Replicator) Name() string {
	return r.name
}

func (r *Replicator) Start() error {
	go func() {
		var mask uint64
		for msg := range r.queueIn {
			mask = r.maskfunc(mask, len(r.queuesOut))
			if err := r.replicate(msg, mask); err != nil {
				msg.Complete(core.MsgStatusFailed)
			}
		}
	}()

	return nil
}

func (r *Replicator) Stop() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	close(r.queueIn)
	for _, q := range r.queuesOut {
		close(q)
	}
	r.wg.Wait()

	return nil
}

func (r *Replicator) Connect(nthreads int, peer core.Receiver) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if len(r.queuesOut) >= MaxPeersCnt {
		return fmt.Errorf("replicator has achieved the max # of connections: %d", len(r.queuesOut))
	}

	q := make(chan *core.Message)
	r.wg.Add(1)
	go func() {
		for msg := range q {
			if err := peer.Receive(msg); err != nil {
				msg.Complete(core.MsgStatusFailed)
				r.ctx.Logger().Error(err.Error())
			}
		}
		r.wg.Done()
	}()
	r.queuesOut = append(r.queuesOut, q)

	return nil
}

func (r *Replicator) Receive(msg *core.Message) error {
	r.queueIn <- msg

	return nil
}
