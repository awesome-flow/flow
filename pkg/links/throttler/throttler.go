package links

import (
	"booking/bmetrics"
	"booking/msgrelay/flow"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Throttler struct {
	Name    string
	key     string
	rps     uint64
	buckets *sync.Map
	*flow.Connector
}

type stts struct {
	budget    int64
	timestamp int64
}

func NewThrottler(name string, params flow.Params) (flow.Link, error) {
	rps, rpsOk := params["rps"]
	if !rpsOk {
		return nil, fmt.Errorf("Throttler params are missing rps")
	}
	th := &Throttler{
		name,
		"",
		uint64(rps.(int)),
		&sync.Map{},
		flow.NewConnector(),
	}
	if key, keyOk := params["msg_key"]; keyOk {
		th.key = key.(string)
	}

	return th, nil
}

func (th *Throttler) Recv(msg *flow.Message) error {
	msgKey := ""
	if len(th.key) > 0 {
		if _, ok := msg.Meta[th.key]; ok {
			msgKey = msg.Meta[th.key]
		}
	}
	bucket, _ := th.buckets.LoadOrStore(msgKey, &stts{
		budget:    int64(th.rps),
		timestamp: time.Now().UnixNano(),
	})
	var t, prevTimestamp, budgetExtra, newBudget, budget int64
	loopBreaker := 10

	for {
		if loopBreaker < 0 {
			break
		}
		t = time.Now().UnixNano()
		prevTimestamp = atomic.LoadInt64(&(bucket.(*stts)).timestamp)
		budget = atomic.LoadInt64(&(bucket.(*stts)).budget)
		budgetExtra = int64(
			math.Round(float64(t-prevTimestamp) *
				float64(th.rps) / float64(time.Second.Nanoseconds())))
		newBudget = budget + budgetExtra - 1
		if newBudget < 0 {
			break
		}
		if newBudget > int64(th.rps) {
			newBudget = int64(th.rps)
		}
		if atomic.CompareAndSwapInt64(&(bucket.(*stts)).timestamp, prevTimestamp, t) {
			// TODO: Race condition here
			atomic.StoreInt64(&(bucket.(*stts)).budget, newBudget)
			bmetrics.GetOrRegisterCounter(
				"links", "throttler", th.Name+"_pass").Inc(1)
			return th.Send(msg)
		}
		loopBreaker--
	}

	bmetrics.GetOrRegisterCounter("links", "throttler", th.Name+"_reject").Inc(1)
	return msg.AckThrottled()
}
