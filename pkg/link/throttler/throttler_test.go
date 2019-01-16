package link

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/awesome-flow/flow/pkg/core"
)

type Nil struct {
	*core.Connector
}

const (
	DefaultCntrPrecision = 0.01 // 1%
)

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
			th, thErr := New(
				"t",
				core.Params{"msg_key": test.msgKey, "rps": test.rps},
				core.NewContext(),
			)
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

func TestThrottler_Recv_Parallel(t *testing.T) {
	tests := []struct {
		parallelRequests int
		rpsLimit         int
	}{
		{10, 1},
		{10, 10},
		{10, 100},
		{10, 1000},

		{100, 1},
		{100, 10},
		{100, 100},
		{100, 1000},

		{1000, 1},
		{1000, 10},
		{1000, 100},
		{1000, 1000},
	}
	for _, test := range tests {
		testName := fmt.Sprintf("parallel %v requests %v limit", test.parallelRequests, test.rpsLimit)
		t.Run(testName, func(t *testing.T) {
			th, thErr := New(
				"t",
				core.Params{"msg_key": "", "rps": test.rpsLimit, "timeFunction": func() int64 { return 0 }},
				core.NewContext(),
			)
			if thErr != nil {
				t.Errorf("Could not instantiate throttler: %s", thErr.Error())
			}
			th.ConnectTo(NewNil())

			cnt := 0
			cntCh := make(chan int)

			var wg sync.WaitGroup
			wg.Add(1)

			for i := 0; i < test.parallelRequests; i++ {
				go func() {
					message := core.NewMessage([]byte(""))
					wg.Wait()
					err := th.Recv(message)

					if err == nil {
						cntCh <- 1
					} else if err == core.ErrMsgThrottled {
						cntCh <- 0
					} else {
						t.Errorf("Unexpected error returned: %s", err.Error())
						cntCh <- 1000
					}
				}()
			}

			wg.Done()

			for i := 0; i < test.parallelRequests; i++ {
				cnt += <-cntCh
			}

			wantCnt := test.parallelRequests
			if test.rpsLimit < test.parallelRequests {
				wantCnt = test.rpsLimit
			}

			if cnt > wantCnt {
				t.Errorf("Unexpected amount of succ sends: %d, want: %d", cnt, wantCnt)
			}

			if !withinPrecisionInterval(cnt, wantCnt, DefaultCntrPrecision) {
				t.Errorf("Unexpected amount of succ sends: %d, want: %d ± %v%%", cnt, wantCnt, DefaultCntrPrecision*100)
			}
		})
	}
}

func withinPrecisionInterval(want, got int, precision float64) bool {
	return math.Abs(float64(want-got)) <= float64(want)*precision
}
