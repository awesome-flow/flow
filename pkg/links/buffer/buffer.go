package links

import (
	"booking/bmetrics"
	"booking/msgrelay/flow"
	"fmt"
	"time"
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
	msgChan  chan *flow.Message
}

func NewBuffer(name string, params flow.Params) (flow.Link, error) {
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
		make(chan *flow.Message, capacity),
	}
	return buf, nil
}

func (buf *Buffer) Recv(msg *flow.Message) error {
	return buf.Send(msg)
}

func (buf *Buffer) Send(msg *flow.Message) error {
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

func (buf *Buffer) ConnectTo(link flow.Link) error {
	go func() {
		for msg := range buf.msgChan {
			if msg.GetAttempts() >= uint32(buf.maxRetry) {
				bmetrics.GetOrRegisterCounter(
					"links", "buffer", buf.Name+"_max_attempts").Inc(1)
				msg.AckFailed()
				continue
			}
			msgCp := flow.CpMessage(msg)
			if recvErr := link.Recv(msgCp); recvErr != nil {
				bmetrics.GetOrRegisterCounter(
					"links", "buffer", buf.Name+"_retry").Inc(1)
				msg.BumpAttempts()
				buf.Send(msg)
				continue
			}
			select {
			case upd := <-msgCp.GetAckCh():
				if upd != flow.MsgStatusDone {
					bmetrics.GetOrRegisterCounter(
						"links", "buffer", buf.Name+"_retry").Inc(1)
					msg.BumpAttempts()
					buf.Send(msg)
					continue
				} else {
					bmetrics.GetOrRegisterCounter(
						"links", "buffer", buf.Name+"_success").Inc(1)
					msg.AckDone()
				}
			case <-time.After(MsgSendTimeout):
				bmetrics.GetOrRegisterCounter(
					"links", "buffer", buf.Name+"_timeout").Inc(1)
				msg.BumpAttempts()
				buf.Send(msg)
				continue
			}
		}
	}()
	return nil
}

func (buf *Buffer) LinkTo([]flow.Link) error {
	panic("Buffer does not support LinkTo()")
}

func (buf *Buffer) RouteTo(map[string]flow.Link) error {
	panic("Buffer does not support RouteTo()")
}

func (buf *Buffer) ExecCmd(cmd *flow.Cmd) error {
	return nil
}
