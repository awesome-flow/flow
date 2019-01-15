package receiver

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	testutils "github.com/awesome-flow/flow/pkg/util/test"
)

func TestHTTP_handleSendV1(t *testing.T) {
	tests := []struct {
		name               string
		bindAddr           string
		reply              testutils.ReplyType
		payload            []byte
		extra              string
		expectedStatusCode int
		isMessageExpected  bool
		expectedPayload    []byte
		expectedMeta       map[string]interface{}
	}{
		{
			"sync empty",
			":17101",
			testutils.ReplyDone,
			[]byte{},
			"",
			http.StatusBadRequest,
			false,
			nil,
			nil,
		},
		{
			"async successful",
			":17102",
			testutils.ReplyDone,
			[]byte("hello world"),
			"",
			http.StatusAccepted,
			true,
			[]byte("hello world"),
			nil,
		},
		{
			"sync successful",
			":17103",
			testutils.ReplyDone,
			[]byte("hello world"),
			"?sync=true",
			http.StatusOK,
			true,
			[]byte("hello world"),
			map[string]interface{}{"sync": "true"},
		},
		{
			"sync partial",
			":17104",
			testutils.ReplyPartialSend,
			[]byte("hello world"),
			"?sync=true",
			http.StatusConflict,
			true,
			[]byte("hello world"),
			map[string]interface{}{"sync": "true"},
		},
		{
			"sync invalid",
			":17105",
			testutils.ReplyInvalid,
			[]byte("hello world"),
			"?sync=true",
			http.StatusBadRequest,
			true,
			[]byte("hello world"),
			map[string]interface{}{"sync": "true"},
		},
		{
			"sync failed",
			":17106",
			testutils.ReplyFailed,
			[]byte("hello world"),
			"?sync=true",
			http.StatusInternalServerError,
			true,
			[]byte("hello world"),
			map[string]interface{}{"sync": "true"},
		},
		{
			"sync timeout",
			":17107",
			testutils.ReplyTimedOut,
			[]byte("hello world"),
			"?sync=true",
			http.StatusGatewayTimeout,
			true,
			[]byte("hello world"),
			map[string]interface{}{"sync": "true"},
		},
		{
			"sync unroutable",
			":17108",
			testutils.ReplyUnroutable,
			[]byte("hello world"),
			"?sync=true",
			http.StatusNotAcceptable,
			true,
			[]byte("hello world"),
			map[string]interface{}{"sync": "true"},
		},
		{
			"sync throttled",
			":17109",
			testutils.ReplyThrottled,
			[]byte("hello world"),
			"?sync=true",
			http.StatusTooManyRequests,
			true,
			[]byte("hello world"),
			map[string]interface{}{"sync": "true"},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {

			httpRcv, err := New("test_http",
				core.Params{"bind_addr": testCase.bindAddr}, core.NewContext())
			if err != nil {
				t.Errorf("Failed to create an HTTP receiver: %s", err)
			}
			time.Sleep(10 * time.Millisecond)
			rcvLink := testutils.NewRememberAndReply("rar", testCase.reply)
			httpRcv.ConnectTo(rcvLink)
			buf := bytes.NewReader(testCase.payload)
			resp, err := http.Post(
				"http://"+testCase.bindAddr+"/send"+testCase.extra,
				"application/octet-stream",
				buf,
			)
			if err != nil {
				t.Errorf("Failed to send an HTTP request: %s", err)
			}
			if resp == nil {
				t.Fatalf("Response is nil")
			}
			_, readErr := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				t.Errorf("Failed to read the body: %s", readErr)
			}
			if resp.StatusCode != testCase.expectedStatusCode {
				t.Errorf("Unexpected HTTP status code: %d, want: %d",
					resp.StatusCode, testCase.expectedStatusCode)
			}

			if !testCase.isMessageExpected {
				return
			}

			received := make(chan struct{})
			go func() {
				for {
					if rcvLink.LastMsg() != nil {
						received <- struct{}{}
						return
					}
				}
			}()

			select {
			case <-received:
			case <-time.After(10 * time.Millisecond):
				t.Fatalf("Failed to receive the message")
			}

			if bytes.Compare(testCase.expectedPayload, rcvLink.LastMsg().Payload) != 0 {
				t.Errorf("Unexpected last message content: %s, want: %s",
					rcvLink.LastMsg().Payload, testCase.expectedPayload)
			}
			httpRcv.ExecCmd(&core.Cmd{Code: core.CmdCodeStop})
		})
	}
}
