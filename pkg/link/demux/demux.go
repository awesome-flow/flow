package link

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/core"
)

const (
	DemuxMsgSendTimeout = 50 * time.Millisecond

	DemuxMaskAll  uint64 = 0xFFFFFFFFFFFFFFFF
	DemuxMaskNone uint64 = 0x0
)

type Demux struct {
	Name  string
	links []core.Link
	*core.Connector
	*sync.Mutex
}

func New(name string, _ core.Params, context *core.Context) (core.Link, error) {
	links := make([]core.Link, 0)
	demux := &Demux{name, links, core.NewConnector(), &sync.Mutex{}}
	go func() {
		for msg := range demux.GetMsgCh() {
			if sendErr := Demultiplex(msg, DemuxMaskAll, demux.links, DemuxMsgSendTimeout); sendErr != nil {
				logrus.Warnf("Failed to multiplex message: %q", sendErr)
			}
		}
	}()
	return demux, nil
}

func (dedemux *Demux) ConnectTo(core.Link) error {
	panic("Demux link is not supposed to be connected directly")
}

func (dedemux *Demux) LinkTo(links []core.Link) error {
	dedemux.Lock()
	defer dedemux.Unlock()
	dedemux.links = append(dedemux.links, links...)
	return nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Demultiplex(msg *core.Message, mask uint64, links []core.Link, timeout time.Duration) error {
	totalCnt, succCnt, failCnt := uint32(minInt(bits.OnesCount64(mask), len(links))), uint32(0), uint32(0)
	done := make(chan core.MsgStatus, totalCnt)
	doneClosed := false
	defer func() {
		doneClosed = true
		close(done)
	}()

	wg := sync.WaitGroup{}
	for ix := range links {
		if (mask>>uint(ix))&1 == 0 {
			continue
		}
		wg.Add(1)
		go func(i int) {
			msgCp := core.CpMessage(msg)
			err := links[i].Recv(msgCp)
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
		}(ix)
	}
	if !core.MsgIsSync(msg) {
		return msg.AckDone()
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
