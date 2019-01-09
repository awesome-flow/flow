package link

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
)

type BufStrategy uint8

const (
	BufStrategyBlock BufStrategy = iota
	BufStrategySub
	BufStrategyDrop
)

const (
	MsgSendTimeout     = 100 * time.Millisecond
	DefaultBufCapacity = 65536
)

type Buffer struct {
	Name     string
	capacity int
	strategy BufStrategy
	maxRetry int
	context  *core.Context
}

func New(name string, params core.Params, context *core.Context) (core.Link, error) {
	capacity := DefaultBufCapacity
	if v, ok := params["capacity"]; ok {
		capacity = v.(int)
	}
	strategy := BufStrategySub
	if v, ok := params["strategy"]; ok {
		switch v.(string) {
		case "block":
			strategy = BufStrategyBlock
		case "sub":
			strategy = BufStrategySub
		case "drop":
			strategy = BufStrategyDrop
		default:
			return nil, fmt.Errorf("Unknown buf strategy: %s", v.(string))
		}
	}
	maxRetry := 1
	if v, ok := params["max_retry"]; ok {
		maxRetry = v.(int)
	}
	threadiness := runtime.GOMAXPROCS(-1)
	msgCh := make([]chan *core.Message, threadiness)
	for i := 0; i < threadiness; i++ {
		msgCh[i] = make(chan *core.Message, capacity)
	}
	ctx := core.NewContextUnsafe(msgCh, nil, nil, &sync.Map{})
	buf := &Buffer{name, capacity, strategy, maxRetry, ctx}

	return buf, nil
}

func (buf *Buffer) GetContext() *core.Context {
	return buf.context
}

func (buf *Buffer) Recv(msg *core.Message) error {
	return buf.Send(msg)
}

func (buf *Buffer) Send(msg *core.Message) error {
	rnd := rand.Intn(len(buf.context.GetMsgCh()))
	switch buf.strategy {
	case BufStrategyDrop:
		if len(buf.context.GetMsgCh()[rnd]) >= buf.capacity {
			return msg.AckFailed()
		}
	case BufStrategySub:
		for len(buf.context.GetMsgCh()) >= buf.capacity {
			msg := <-buf.context.GetMsgCh()[rnd]
			msg.AckFailed()
		}
	}

	buf.context.GetMsgCh()[rnd] <- msg

	return nil
}

func (buf *Buffer) ConnectTo(link core.Link) error {
	for _, ch := range buf.context.GetMsgCh() {
		go func(ch chan *core.Message) {
			for msg := range ch {
				if msg.GetAttempts() >= uint32(buf.maxRetry) {
					metrics.GetCounter(
						"links.buffer," + buf.Name + "_max_attempts").Inc(1)
					msg.AckFailed()
					continue
				}
				msgCp := core.CpMessage(msg)
				if recvErr := link.Recv(msgCp); recvErr != nil {
					metrics.GetCounter(
						"links.buffer." + buf.Name + "_retry").Inc(1)
					msg.BumpAttempts()
					buf.Send(msg)
					continue
				}
				select {
				case upd := <-msgCp.GetAckCh():
					if upd != core.MsgStatusDone {
						metrics.GetCounter(
							"links.buffer." + buf.Name + "_retry").Inc(1)
						msg.BumpAttempts()
						buf.Send(msg)
						continue
					} else {
						metrics.GetCounter(
							"links.buffer." + buf.Name + "_success").Inc(1)
						msg.AckDone()
					}
				case <-time.After(MsgSendTimeout):
					metrics.GetCounter(
						"links.buffer." + buf.Name + "_timeout").Inc(1)
					msg.BumpAttempts()
					buf.Send(msg)
					continue
				}
			}
		}(ch)
	}
	return nil
}

func (buf *Buffer) LinkTo([]core.Link) error {
	panic("Buffer does not support LinkTo()")
}

func (buf *Buffer) RouteTo(map[string]core.Link) error {
	panic("Buffer does not support RouteTo()")
}

func (buf *Buffer) ExecCmd(cmd *core.Cmd) error {
	return nil
}

func (buf *Buffer) String() string {
	return buf.Name
}
