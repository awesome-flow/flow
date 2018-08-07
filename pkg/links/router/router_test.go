package links

import (
	"booking/msgrelay/flow"
	"testing"
	"time"
)

type A struct {
	rcvCnt int
	*flow.Connector
}

func NewA() *A {
	return &A{0, flow.NewConnector()}
}

func (a *A) Recv(msg *flow.Message) error {
	a.rcvCnt++
	return msg.AckDone()
}

func TestRouter_Dispatch(t *testing.T) {
	a1, a2 := NewA(), NewA()
	r, rErr := NewRouter(
		"router",
		flow.Params{"routing_key": "sender"},
	)
	if rErr != nil {
		t.Errorf("Failed to initialize router: %s", rErr.Error())
	}
	if linkErr := r.RouteTo(map[string]flow.Link{"a1": a1, "a2": a2}); linkErr != nil {
		t.Errorf("Failed to link router: %s", linkErr.Error())
	}
	m1 := flow.NewMessage(map[string]string{"sender": "a1"}, []byte(""))
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
	m2 := flow.NewMessage(map[string]string{"sender": "a2"}, []byte(""))
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
	m3 := flow.NewMessage(map[string]string{"sender": "a3"}, []byte(""))
	r.Send(m3)

	select {
	case s := <-m3.GetAckCh():
		if s != flow.MsgStatusUnroutable {
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
