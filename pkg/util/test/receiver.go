package test

import "github.com/whiteboxio/flow/pkg/core"

type ReplyType uint8

const (
	ReplyContinue ReplyType = iota
	ReplyDone
	ReplyInvalid
	ReplyPartialSend
	ReplyFailed
	ReplyTimedOut
	ReplyUnroutable
	ReplyThrottled
)

type CountAndReply struct {
	Name   string
	RcvCnt int
	Reply  ReplyType
	*core.Connector
}

func NewCountAndReply(name string, reply ReplyType) *CountAndReply {
	return &CountAndReply{
		name,
		0,
		reply,
		core.NewConnector(),
	}
}

func (car *CountAndReply) Recv(msg *core.Message) error {
	car.RcvCnt++
	switch car.Reply {
	case ReplyDone:
		return msg.AckDone()
	case ReplyInvalid:
		return msg.AckInvalid()
	case ReplyPartialSend:
		return msg.AckPartialSend()
	case ReplyFailed:
		return msg.AckFailed()
	case ReplyTimedOut:
		return msg.AckTimedOut()
	case ReplyUnroutable:
		return msg.AckUnroutable()
	case ReplyThrottled:
		return msg.AckThrottled()
	default:
		return msg.AckContinue()
	}
}

func InitCountAndReplySet(namereplies map[string]ReplyType) []core.Link {
	res := make([]core.Link, len(namereplies))
	ix := 0
	for name, reply := range namereplies {
		car := NewCountAndReply(name, reply)
		res[ix] = car
		ix++
	}
	return res
}
