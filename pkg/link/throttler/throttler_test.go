package link

import (
	"testing"

	"github.com/whiteboxio/flow/pkg/core"
)

type Nil struct {
	*core.Connector
}

func NewNil() *Nil                          { return &Nil{core.NewConnector()} }
func (n *Nil) Recv(msg *core.Message) error { return msg.AckDone() }

func TestThrottler_Recv(t *testing.T) {
	tests := []struct {
		name         string
		msgKey       string
		rps          int
		expectedSucc int
	}{
		{"1 per second", "", 1, 1},
		{"10 per second", "", 10, 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			th, thErr := NewThrottler("t",
				core.Params{"msg_key": test.msgKey, "rps": test.rps})
			if thErr != nil {
				t.Errorf("Could not instantiate throttler: %s", thErr.Error())
			}
			th.ConnectTo(NewNil())
			cnt := 0
			var err error
			for {
				if err = th.Recv(core.NewMessage([]byte(""))); err != nil {
					break
				}
				cnt++
			}
			if err != core.ErrMsgThrottled {
				t.Errorf("Unexpected error returned: %s", err.Error())
			}
			if cnt != test.expectedSucc {
				t.Errorf("Unexpected amount of succ sends: %d, want: %d", cnt, test.expectedSucc)
			}
		})
	}
}
