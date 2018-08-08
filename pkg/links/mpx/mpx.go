package links

import (
	"sync"
	"time"

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

func NewMPX(name string, _ core.Params) (core.Link, error) {
	links := make([]core.Link, 0)
	mpx := &MPX{name, links, core.NewConnector(), &sync.Mutex{}}
	go mpx.multiplex()
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

func (mpx *MPX) multiplex() {
	for msg := range mpx.GetMsgCh() {
		mpx.Lock()
		linksLen := len(mpx.links)
		acks := make(chan core.MsgStatus, linksLen)
		ackChClosed := false
		for _, link := range mpx.links {
			go func(l core.Link) {
				msgCp := core.NewMessage(msg.Meta, msg.Payload)
				if sendErr := l.Recv(msgCp); sendErr != nil {
					acks <- core.MsgStatusFailed
					return
				}
				for ack := range msgCp.GetAckCh() {
					if !ackChClosed {
						acks <- ack
					}
				}
			}(link)
		}
		mpx.Unlock()
		ackCnt := 0
		failedCnt := 0
		for {
			if ackCnt == linksLen {
				break
			}
			select {
			case s := <-acks:
				ackCnt++
				if s != core.MsgStatusDone {
					failedCnt++
				}
			case <-time.After(MpxMsgSendTimeout):
				ackCnt++
				failedCnt++
			}
		}
		if failedCnt == 0 {
			msg.AckDone()
		} else if failedCnt == linksLen {
			msg.AckFailed()
		} else {
			msg.AckPartialSend()
		}
		ackChClosed = true
		for len(acks) > 0 {
			<-acks
		}
		close(acks)
	}
}
