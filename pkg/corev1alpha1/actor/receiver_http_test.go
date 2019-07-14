package actor

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

type testResponseWriter struct {
	headers map[string][]string
	status  int
	bytes.Buffer
}

var _ http.ResponseWriter = (*testResponseWriter)(nil)

func (rw *testResponseWriter) Header() http.Header {
	return rw.headers
}

func (rw *testResponseWriter) WriteHeader(status int) {
	rw.status = status
}

func TestHandleReqV1alpha1(t *testing.T) {
	nthreads := 4

	type teststruct struct {
		name      string
		url       string
		reqbody   []byte
		msgstatus core.MsgStatus
		lag       time.Duration
		expcode   int
		expresp   string
	}

	tests := make([]teststruct, 0, len(MsgStatusToHttpResp)+1)

	sts2name := func(sts core.MsgStatus) string {
		switch sts {
		case core.MsgStatusDone:
			return "MsgStatusDone"
		case core.MsgStatusFailed:
			return "MsgStatusFailed"
		case core.MsgStatusTimedOut:
			return "MsgStatusTimedOut"
		case core.MsgStatusUnroutable:
			return "MsgStatusUnroutable"
		case core.MsgStatusThrottled:
			return "MsgStatusThrottled"
		default:
			return "Unknown"
		}
	}

	for sts, ct := range MsgStatusToHttpResp {
		tests = append(tests, teststruct{
			name:      fmt.Sprintf("%s to %d %s", sts2name(sts), ct.code, ct.text),
			url:       "http://example.com",
			reqbody:   testutil.RandBytes(1024),
			msgstatus: sts,
			expcode:   ct.code,
			expresp:   string(ct.text),
		})
	}

	lag := 100 * time.Millisecond
	tests = append(tests, teststruct{
		name:      fmt.Sprintf("lag of %d to %d %s", lag, http.StatusGatewayTimeout, "Timed out to send message"),
		url:       "http://example.com",
		reqbody:   testutil.RandBytes(1024),
		msgstatus: core.MsgStatusTimedOut,
		lag:       lag,
		expcode:   http.StatusGatewayTimeout,
		expresp:   "Timed out to send message",
	})

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			url, err := url.Parse(testCase.url)
			if err != nil {
				t.Fatalf("failed to parse url %q: %s", testCase.url, err)
			}

			req := &http.Request{
				Body:          ioutil.NopCloser(bytes.NewBuffer(testCase.reqbody)),
				ContentLength: int64(len(testCase.reqbody)),
				URL:           url,
			}

			repo := cfg.NewRepository()
			ctx, err := core.NewContext(core.NewConfig(repo))
			if err != nil {
				t.Fatalf("failed to create context: %s", err)
			}
			if err := ctx.Start(); err != nil {
				t.Fatalf("failed to start context: %s", err)
			}

			rcv, err := NewReceiverHTTP(
				"receiver-http",
				ctx,
				core.Params{"bind": "0.0.0.0:8080"}, // bind is a mock, we are not starting the server
			)
			if err != nil {
				t.Fatalf("failed to create receiver: %s", err)
			}

			peer, err := flowtest.NewTestActor("test-actor", ctx, core.Params{})
			if err != nil {
				t.Fatalf("failed to start test actor: %s", err)
			}
			if err := rcv.Connect(nthreads, peer); err != nil {
				t.Fatalf("failed to connect test actor: %s", err)
			}
			peer.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
				if testCase.lag > 0 {
					time.Sleep(testCase.lag)
				}
				msg.Complete(testCase.msgstatus)
				peer.(*flowtest.TestActor).Flush()
			})

			rw := &testResponseWriter{}
			rcv.(*ReceiverHTTP).handleReqV1alpha1(rw, req)

			if rw.status != testCase.expcode {
				t.Fatalf("unexpected http status: got: %d, want: %d", rw.status, http.StatusOK)
			}

			if resp := rw.String(); resp != testCase.expresp {
				t.Fatalf("unexpected http response: got: %s, want: %s", resp, "OK")
			}
		})
	}
}
