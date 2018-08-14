package link

import (
	"fmt"
	"time"

	"github.com/whiteboxio/flow/pkg/core"
	"github.com/whiteboxio/flow/pkg/metrics"
)

type BufStrategy uint8

const (
	BufStrategyBlock BufStrategy = iota
	BufStrategySub
	BufStrategyDrop
)

const (
	MsgSendTimeout = 100 * time.Millisecond
)

type Buffer struct {
	Name     string
	capacity int
	strategy BufStrategy
	maxRetry int
	msgChan  chan *core.Message
}

func NewBuffer(name string, params core.Params) (core.Link, error) {
	capacity := 65536
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
	buf := &Buffer{
		name,
		capacity,
		strategy,
		maxRetry,
		make(chan *core.Message, capacity),
	}
	return buf, nil
}

func (buf *Buffer) Recv(msg *core.Message) error {
	return buf.Send(msg)
}

func (buf *Buffer) Send(msg *core.Message) error {
	switch buf.strategy {
	case BufStrategyDrop:
		if len(buf.msgChan) >= buf.capacity {
			return msg.AckFailed()
		}
	case BufStrategySub:
		for len(buf.msgChan) >= buf.capacity {
			msg := <-buf.msgChan
			msg.AckFailed()
		}
	}

	buf.msgChan <- msg

	return nil
}

func (buf *Buffer) ConnectTo(link core.Link) error {
	go func() {
		for msg := range buf.msgChan {
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
	}()
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
