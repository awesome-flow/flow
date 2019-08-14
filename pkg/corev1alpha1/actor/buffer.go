package actor

import (
	"fmt"
	"sync"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
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
	name  string
	ctx   *core.Context
	queue chan *MsgCnt
	wg    sync.WaitGroup
}

var _ core.Actor = (*Buffer)(nil)

func NewBuffer(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &Buffer{
		name:  name,
		ctx:   ctx,
		queue: make(chan *MsgCnt, DefaultBufCapacity),
	}, nil
}

func (b *Buffer) Name() string {
	return b.name
}

func (b *Buffer) Start() error {
	return nil
}

func (b *Buffer) Stop() error {
	close(b.queue)
	b.wg.Wait()

	return nil
}

func (b *Buffer) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		b.wg.Add(1)
		go func() {
			var sts core.MsgStatus
			for msgcnt := range b.queue {
				msgcp := msgcnt.msg.Copy()
				err := peer.Receive(msgcp)
				sts = 0
				if err == nil {
					sts = msgcp.Await()
					switch sts {
					case core.MsgStatusDone, core.MsgStatusPartialSend:
					default:
						err = fmt.Errorf("failed to send message: code(%d)", sts)
					}
				}
				if err != nil {
					msgcnt.cnt++
					if msgcnt.cnt < DefaultBufMaxAttempts {
						b.queue <- msgcnt
						continue
					}
					sts = core.MsgStatusFailed
				}
				msgcnt.msg.Complete(sts)
			}
			b.wg.Done()
		}()
	}

	return nil
}

func (b *Buffer) Receive(msg *core.Message) error {
	b.queue <- NewMsgCnt(msg)

	return nil
}
