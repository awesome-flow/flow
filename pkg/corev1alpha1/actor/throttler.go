package actor

import (
	"fmt"
	"sync"
	"sync/atomic"
	_ "unsafe"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

//go:noescape
//go:linkname nanotime runtime.nanotime
func nanotime() int64

type Throttler struct {
	name    string
	ctx     *core.Context
	msgkey  string
	msgcost int64
	bukcap  int64
	timefun func() int64
	buckets map[string]*int64
	mutex   sync.RWMutex
	queue   chan *core.Message
	wg      sync.WaitGroup
}

var _ core.Actor = (*Throttler)(nil)

func NewThrottler(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	rps, ok := params["rps"]
	if !ok {
		return nil, fmt.Errorf("throttler %s is missing `rps` config", name)
	}

	msgcost := 1000000000 / int64(rps.(int))
	bukcap := 1000000000 - msgcost

	t := &Throttler{
		name:    name,
		ctx:     ctx,
		msgcost: msgcost,
		bukcap:  bukcap,
		timefun: nanotime,
		buckets: map[string]*int64{"": new(int64)},
		queue:   make(chan *core.Message),
	}

	if msgkey, ok := params["msgkey"]; ok {
		t.msgkey = msgkey.(string)
	}

	return t, nil
}

func (t *Throttler) Name() string {
	return t.name
}

func (t *Throttler) Start() error {
	return nil
}

func (t *Throttler) Stop() error {
	close(t.queue)
	t.wg.Wait()

	return nil
}

func (t *Throttler) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		t.wg.Add(1)
		go func() {
			for msg := range t.queue {
				if err := peer.Receive(msg); err != nil {
					t.ctx.Logger().Error(err.Error())
				}
			}
			t.wg.Done()
		}()
	}

	return nil
}

func (t *Throttler) Receive(msg *core.Message) error {
	k := ""
	if len(t.msgkey) > 0 {
		if v, ok := msg.Meta(t.msgkey); ok {
			k = v.(string)
		}
	}
	if t.shouldPass(k) {
		t.queue <- msg
		return nil
	}
	msg.Complete(core.MsgStatusThrottled)

	return nil
}

func (t *Throttler) getBucket(msgkey string) *int64 {
	t.mutex.RLock()
	if bucket, ok := t.buckets[msgkey]; ok {
		t.mutex.RUnlock()
		return bucket
	}

	b := new(int64)
	*b = t.timefun() - 1

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if bucket, ok := t.buckets[msgkey]; ok {
		return bucket
	}
	t.buckets[msgkey] = b

	return b
}

func (t *Throttler) shouldPass(msgkey string) bool {
	bucket := t.getBucket(msgkey)

	for l := 0; l < 10; l++ {
		now := t.timefun()
		tat := atomic.LoadInt64(bucket) // theoretical arrival time
		if now < tat-t.bukcap {
			return false
		}
		var newtat int64
		if now > tat {
			newtat = now + t.msgcost
		} else {
			newtat = tat + t.msgcost
		}

		if atomic.CompareAndSwapInt64(bucket, tat, newtat) {
			return true
		}
	}

	return false
}
