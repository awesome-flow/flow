package link

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/sirupsen/logrus"
)

const (
	MsgSendTimeout = 50 * time.Millisecond

	DemuxMaskAll  uint64 = 0xFFFFFFFFFFFFFFFF
	DemuxMaskNone uint64 = 0x0
)

type Demux struct {
	Name  string
	links []core.Link
	*sync.Mutex
	*core.Connector
}

func New(name string, _ types.Params, context *core.Context) (core.Link, error) {
	demux := &Demux{
		name,
		nil,
		&sync.Mutex{},
		core.NewConnectorWithContext(context),
	}

	demux.OnSetUp(demux.SetUp)
	demux.OnTearDown(demux.TearDown)

	return demux, nil
}

func (demux *Demux) SetUp() error {
	for _, ch := range demux.MsgCh() {
		go func(ch chan *core.Message) {
			for msg := range ch {
				if sendErr := Demultiplex(msg, DemuxMaskAll, demux.links, MsgSendTimeout); sendErr != nil {
					logrus.Warnf("Failed to multiplex message: %q", sendErr)
				}
			}
		}(ch)
	}
	return nil
}

func (demux *Demux) TearDown() error {
	for _, ch := range demux.MsgCh() {
		close(ch)
	}
	return nil
}

func (demux *Demux) ConnectTo(core.Link) error {
	panic("Demux link is not supposed to be connected directly")
}

func (demux *Demux) LinkTo(links []core.Link) error {
	demux.Lock()
	defer demux.Unlock()
	demux.links = append(demux.links, links...)

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

func Demultiplex(msg *core.Message, active uint64, links []core.Link, timeout time.Duration) error {

	totalCnt, failCnt := uint32(minInt(bits.OnesCount64(active), len(links))), uint32(0)
	msgIsSync := core.MsgIsSync(msg)

	wgSend := sync.WaitGroup{}
	wgAck := sync.WaitGroup{}

	for ix := range links {
		if (active & (1 << uint(ix))) == 0 {
			continue
		}
		wgSend.Add(1)
		wgAck.Add(1)
		go func(i int) {
			msgCp := core.CpMessage(msg)
			err := links[i].Recv(msgCp)
			wgSend.Done()
			if !msgIsSync {
				return
			}
			if err != nil {
				atomic.AddUint32(&failCnt, 1)
			} else {
				status := <-msgCp.AckCh()
				if status != core.MsgStatusDone {
					atomic.AddUint32(&failCnt, 1)
				}
			}
			wgAck.Done()
		}(ix)
	}

	wgSend.Wait()

	if msgIsSync {
		done := make(chan uint32)
		defer close(done)
		go func() {
			wgAck.Wait()
			done <- totalCnt - failCnt
		}()
		select {
		case succCnt := <-done:
			if succCnt < totalCnt {
				if atomic.LoadUint32(&succCnt) == 0 {
					return msg.AckFailed()
				}
				return msg.AckPartialSend()
			}
			return msg.AckDone()
		case <-time.After(timeout):
			return msg.AckTimedOut()
		}
	}

	return msg.AckDone()
}
