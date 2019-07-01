package actor

import (
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
)

const (
	DefaultBufCapacity    = 65536
	DefaultBufMaxAttempts = 16
)

type MsgCnt struct {
	msg *core.Message
	cnt uint32
}

func NewMsgCnt(msg *core.Message) *MsgCnt {
	return &MsgCnt{msg: msg}
}

type Buffer struct {
	name     string
	ctx      *core.Context
	queueIn  chan *MsgCnt
	queueOut chan *core.Message
}

var _ core.Actor = (*Buffer)(nil)

func NewBuffer(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &Buffer{
		name:     name,
		ctx:      ctx,
		queueIn:  make(chan *MsgCnt, DefaultBufCapacity),
		queueOut: make(chan *core.Message),
	}, nil
}

func (b *Buffer) Name() string {
	return b.name
}

func (b *Buffer) Start() error {
	nthreads, _ := b.ctx.Config().Get(types.NewKey("system.maxprocs"))
	for i := 0; i < nthreads.(int); i++ {
		go func() {
			for msgcnt := range b.queueIn {
				msgcp := msgcnt.msg.Copy()
				done := msgcp.AwaitChan()
				b.queueOut <- msgcp
				sts := <-done
				msgcnt.cnt++
				if sts != core.MsgStatusDone &&
					sts != core.MsgStatusPartialSend &&
					msgcnt.cnt < DefaultBufMaxAttempts {
					b.queueIn <- msgcnt
					continue
				}
				if err := msgcnt.msg.Complete(sts); err != nil {
					b.ctx.Logger().Error("failed to complete mesage: %s", err)
				}
			}
		}()
	}

	return nil
}

func (b *Buffer) Stop() error {
	close(b.queueIn)
	close(b.queueOut)

	return nil
}

func (b *Buffer) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range b.queueOut {
				if err := peer.Receive(msg); err != nil {
					b.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}

	return nil
}

func (b *Buffer) Receive(msg *core.Message) error {
	b.queueIn <- NewMsgCnt(msg)

	return nil
}
