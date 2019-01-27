package core_test

import (
	"sync"

	"github.com/awesome-flow/flow/pkg/core"
)

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

func replyToMsgStatus(reply ReplyType, msg *core.Message) error {
	switch reply {
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

type CountAndReply struct {
	Name   string
	rcvcnt int
	reply  ReplyType
	mx     *sync.Mutex
	*core.Connector
}

func NewCountAndReply(name string, reply ReplyType) *CountAndReply {
	return &CountAndReply{
		name,
		0,
		reply,
		&sync.Mutex{},
		core.NewConnector(),
	}
}

func (car *CountAndReply) Recv(msg *core.Message) error {
	car.mx.Lock()
	defer car.mx.Unlock()
	car.rcvcnt++
	return replyToMsgStatus(car.reply, msg)
}

func (car *CountAndReply) RcvCnt() int {
	car.mx.Lock()
	defer car.mx.Unlock()
	return car.rcvcnt
}

type RememberAndReply struct {
	Name    string
	lastmsg *core.Message
	reply   ReplyType
	mx      *sync.Mutex
	*core.Connector
}

func NewRememberAndReply(name string, reply ReplyType) *RememberAndReply {
	return &RememberAndReply{name, nil, reply, &sync.Mutex{}, core.NewConnector()}
}

func (rar *RememberAndReply) Recv(msg *core.Message) error {
	rar.mx.Lock()
	defer rar.mx.Unlock()
	rar.lastmsg = msg
	return replyToMsgStatus(rar.reply, msg)
}

func (rar *RememberAndReply) LastMsg() *core.Message {
	rar.mx.Lock()
	defer rar.mx.Unlock()
	return rar.lastmsg
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
