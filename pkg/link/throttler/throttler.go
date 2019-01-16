package link

import (
	"fmt"
	"sync"
	"sync/atomic"
	_ "unsafe"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
)

//go:noescape
//go:linkname nanotime runtime.nanotime
func nanotime() int64

// GCRA-based throttler https://jameslao.com/post/gcra-rate-limiting/
type Throttler struct {
	Name           string
	key            string
	messageCost    int64
	bucketCapacity int64
	mx             sync.RWMutex
	buckets        map[string]*int64
	*core.Connector
}

func New(name string, params core.Params, context *core.Context) (core.Link, error) {
	rps, rpsOk := params["rps"]
	if !rpsOk {
		return nil, fmt.Errorf("Throttler params are missing rps")
	}

	const nanosecondsPerSecond = 1000000000
	nanosecondsPerRequest := nanosecondsPerSecond / int64(rps.(int)) // T
	bucketCapacity := nanosecondsPerSecond - nanosecondsPerRequest   // τ

	th := &Throttler{
		name,
		"",
		nanosecondsPerRequest,
		bucketCapacity,
		sync.RWMutex{},
		map[string]*int64{"": new(int64)},
		core.NewConnector(),
	}
	*th.buckets[""] = nanotime() - 1

	if key, keyOk := params["msg_key"]; keyOk {
		th.key = key.(string)
	}

	return th, nil
}

func (th *Throttler) getOrCreateBucket(key string) *int64 {
	th.mx.RLock()
	bucket, ok := th.buckets[key]
	th.mx.RUnlock()

	if ok {
		return bucket
	}

	// Slow path: create a new bucket and try to insert it
	newBucket := new(int64)
	*newBucket = nanotime() - 1

	th.mx.Lock()
	defer th.mx.Unlock()

	bucket, ok = th.buckets[key]
	if ok {
		return bucket
	} else {
		th.buckets[key] = newBucket
		return newBucket
	}
}

func (th *Throttler) shouldPassMessageWithKey(key string) bool {
	messageCost := th.messageCost       // T
	bucketCapacity := th.bucketCapacity // τ
	bucket := th.getOrCreateBucket(key)

	for loopBreaker := 0; loopBreaker < 10; loopBreaker++ {
		now := nanotime()
		tat := atomic.LoadInt64(bucket) // theoretical arrival time
		if now < tat-bucketCapacity {
			return false
		}

		var newTat int64
		if now > tat {
			newTat = now + messageCost
		} else {
			newTat = tat + messageCost
		}

		if atomic.CompareAndSwapInt64(bucket, tat, newTat) {
			return true
		}
	}

	return false
}

func (th *Throttler) Recv(msg *core.Message) error {
	msgKey := ""
	if len(th.key) > 0 {
		if v, ok := msg.Meta(th.key); ok {
			msgKey = v.(string)
		}
	}

	if th.shouldPassMessageWithKey(msgKey) {
		metrics.GetCounter("links.throttler.msg." + th.Name + "_pass").Inc(1)
		return th.Send(msg)
	} else {
		metrics.GetCounter("links.throttler.msg." + th.Name + "_reject").Inc(1)
		return msg.AckThrottled()
	}
}
