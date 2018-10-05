package link

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/core"
)

const (
	MpxMsgSendTimeout = 50 * time.Millisecond
)

type MPX struct {
	Name  string
	links []core.Link
	*core.Connector
	*sync.Mutex
}

func New(name string, _ core.Params) (core.Link, error) {
	links := make([]core.Link, 0)
	mpx := &MPX{name, links, core.NewConnector(), &sync.Mutex{}}
	go func() {
		for msg := range mpx.GetMsgCh() {
			if sendErr := Multiplex(msg, mpx.links, MpxMsgSendTimeout); sendErr != nil {
				logrus.Warnf("Failed to multiplex message: %q", sendErr)
			}
		}
	}()
	return mpx, nil
}

func (mpx *MPX) ConnectTo(core.Link) error {
	panic("MPX link is not supposed to be connected directly")
}

func (mpx *MPX) LinkTo(links []core.Link) error {
	mpx.Lock()
	defer mpx.Unlock()
	mpx.links = append(mpx.links, links...)
	return nil
}

func Multiplex(msg *core.Message, links []core.Link, timeout time.Duration) error {
	var totalCnt, succCnt, failCnt uint32 = uint32(len(links)), 0, 0
	done := make(chan core.MsgStatus, totalCnt)
	doneClosed := false
	defer func() {
		doneClosed = true
		close(done)
	}()

	wg := sync.WaitGroup{}
	for _, l := range links {
		wg.Add(1)
		go func(link core.Link) {
			msgCp := core.CpMessage(msg)
			err := link.Recv(msgCp)
			wg.Done()
			if err != nil {
				atomic.AddUint32(&failCnt, 1)
				if !doneClosed {
					done <- core.MsgStatusFailed
				}
			}
			status := <-msgCp.GetAckCh()
			if !doneClosed {
				done <- status
			}
		}(l)
	}
	wg.Wait()
	brk := time.After(timeout)
	for i := 0; uint32(i) < totalCnt; i++ {
		select {
		case status := <-done:
			if status == core.MsgStatusDone {
				atomic.AddUint32(&succCnt, 1)
			} else {
				atomic.AddUint32(&failCnt, 1)
			}
		case <-brk:
			return msg.AckTimedOut()
		}
	}

	if failCnt > 0 {
		if succCnt == 0 {
			return msg.AckFailed()
		}
		return msg.AckPartialSend()
	}

	return msg.AckDone()
}
