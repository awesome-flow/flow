package link

import (
	"testing"
	"time"

	"github.com/whiteboxio/flow/pkg/core"
)

type A struct {
	rcvCnt int
	*core.Connector
}

func NewA() *A {
	return &A{0, core.NewConnector()}
}

func (a *A) Recv(msg *core.Message) error {
	a.rcvCnt++
	return msg.AckDone()
}

func TestRouter_Dispatch(t *testing.T) {
	a1, a2 := NewA(), NewA()
	r, rErr := New(
		"router",
		core.Params{"routing_key": "sender"},
		core.NewContext(),
	)
	if rErr != nil {
		t.Errorf("Failed to initialize router: %s", rErr.Error())
	}
	if linkErr := r.RouteTo(map[string]core.Link{"a1": a1, "a2": a2}); linkErr != nil {
		t.Errorf("Failed to link router: %s", linkErr.Error())
	}
	m1 := core.NewMessageWithMeta(map[string]interface{}{"sender": "a1"}, []byte(""))
	r.Send(m1)
	select {
	case <-m1.GetAckCh():
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Timed out to receive an ack from m1")
	}
	if a1.rcvCnt != 1 {
		t.Errorf("Unexpected counter value in a1: %d", a1.rcvCnt)
	}
	if a2.rcvCnt != 0 {
		t.Errorf("Unexpected counter value in a2: %d", a2.rcvCnt)
	}
	m2 := core.NewMessageWithMeta(map[string]interface{}{"sender": "a2"}, []byte(""))
	r.Send(m2)
	select {
	case <-m2.GetAckCh():
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Timed out to receive an ack from m2")
	}
	if a1.rcvCnt != 1 {
		t.Errorf("Unexpected counter value in a1: %d", a1.rcvCnt)
	}
	if a2.rcvCnt != 1 {
		t.Errorf("Unexpected counter value in a2: %d", a2.rcvCnt)
	}
	m3 := core.NewMessageWithMeta(map[string]interface{}{"sender": "a3"}, []byte(""))
	r.Send(m3)

	select {
	case s := <-m3.GetAckCh():
		if s != core.MsgStatusUnroutable {
			t.Errorf("Unexpected msg return status: %d", s)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Timed out to receive an ack from m3")
	}

	if a1.rcvCnt != 1 {
		t.Errorf("Unexpected counter value in a1: %d", a1.rcvCnt)
	}
	if a2.rcvCnt != 1 {
		t.Errorf("Unexpected counter value in a2: %d", a2.rcvCnt)
	}
}
