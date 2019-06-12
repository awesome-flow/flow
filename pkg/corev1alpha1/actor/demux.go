package actor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

const (
	MsgDemuxTimeout = 50 * time.Millisecond

	DemuxMaskAll  uint64 = 0xFFFFFFFFFFFFFFFF
	DemuxMaskNone uint64 = 0x0
)

type Demux struct {
	name   string
	ctx    *core.Context
	queues []chan *core.Message
	nq     uint8
	mutex  sync.Mutex
}

var _ core.Actor = (*Demux)(nil)

func NewDemux(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &Demux{
		name:   name,
		ctx:    ctx,
		queues: make([]chan *core.Message, 0, 64),
	}, nil
}

func (d *Demux) Name() string {
	return d.name
}

func (d *Demux) Start() error {
	return nil
}

func (d *Demux) Stop() error {
	return nil
}

func (d *Demux) Connect(nthreads int, peer core.Receiver) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.nq >= 64 {
		return fmt.Errorf("demultiplexer has achieved the max # of connections")
	}
	q := make(chan *core.Message)
	d.queues = append(d.queues, q)
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range q {
				if err := peer.Receive(msg); err != nil {
					d.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}
	d.nq = uint8(len(d.queues))

	return nil
}

func (d *Demux) Receive(msg *core.Message) error {
	nq := d.nq
	mask := (1 << nq) - 1
	var failcnt, succcnt uint32 = 0, 0
	ix := 0
	wgsend := sync.WaitGroup{}
	wgack := sync.WaitGroup{}
	for mask > 0 {
		if mask&1 > 0 {
			cpmsg := msg.Copy()
			wgsend.Add(1)
			wgack.Add(1)
			go func(ix int, msg *core.Message) {
				d.queues[ix] <- cpmsg
				wgsend.Done()
				s := <-cpmsg.AwaitChan()
				if s != core.MsgStatusDone {
					atomic.AddUint32(&failcnt, 1)
				} else {
					atomic.AddUint32(&succcnt, 1)
				}
				wgack.Done()
			}(ix, cpmsg)
		}
		mask >>= 1
		ix++
	}
	wgsend.Wait()
	done := make(chan struct{})
	go func() {
		wgack.Wait()
		close(done)
	}()
	select {
	case <-done:
		if succcnt < uint32(nq) {
			if succcnt == 0 {
				msg.Complete(core.MsgStatusFailed)
				return nil
			}
			msg.Complete(core.MsgStatusPartialSend)
			return nil
		}
		msg.Complete(core.MsgStatusDone)
		return nil
	case <-time.After(MsgDemuxTimeout):
		msg.Complete(core.MsgStatusTimedOut)
	}
	return nil
}
