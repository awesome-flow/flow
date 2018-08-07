package links

import (
	"booking/msgrelay/flow"
	"sync"
	"time"
)

const (
	MpxMsgSendTimeout = 50 * time.Millisecond
)

type MPX struct {
	Name  string
	links []flow.Link
	*flow.Connector
	*sync.Mutex
}

func NewMPX(name string, _ flow.Params) (flow.Link, error) {
	links := make([]flow.Link, 0)
	mpx := &MPX{name, links, flow.NewConnector(), &sync.Mutex{}}
	go mpx.multiplex()
	return mpx, nil
}

func (mpx *MPX) ConnectTo(flow.Link) error {
	panic("MPX link is not supposed to be connected directly")
}

func (mpx *MPX) LinkTo(links []flow.Link) error {
	mpx.Lock()
	defer mpx.Unlock()
	mpx.links = append(mpx.links, links...)
	return nil
}

func (mpx *MPX) multiplex() {
	for msg := range mpx.GetMsgCh() {
		mpx.Lock()
		linksLen := len(mpx.links)
		acks := make(chan flow.MsgStatus, linksLen)
		ackChClosed := false
		for _, link := range mpx.links {
			go func(l flow.Link) {
				msgCp := flow.NewMessage(msg.Meta, msg.Payload)
				if sendErr := l.Recv(msgCp); sendErr != nil {
					acks <- flow.MsgStatusFailed
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
				if s != flow.MsgStatusDone {
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
