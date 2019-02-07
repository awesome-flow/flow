package link

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
)

type A struct {
	lastMsg *core.Message
	*core.Connector
}

func NewA() *A {
	return &A{
		nil, core.NewConnector(),
	}
}

func (a *A) Recv(msg *core.Message) error {
	a.lastMsg = msg
	return msg.AckDone()
}

func TestMetaParser_Recv(t *testing.T) {
	tests := []struct {
		name       string
		payload    []byte
		expMeta    map[string]interface{}
		expPayload []byte
	}{
		{
			name:       "empty payload",
			payload:    []byte{},
			expMeta:    map[string]interface{}{},
			expPayload: []byte{},
		},
		{
			name:       "no space delimiter",
			payload:    []byte("{\"foo\":\"bar\"}"),
			expMeta:    map[string]interface{}{},
			expPayload: []byte("{\"foo\":\"bar\"}"),
		},
		{
			name:       "basic meta with unique values",
			payload:    []byte("foo=bar&baz=bar {\"foo\":\"bar\"}"),
			expMeta:    map[string]interface{}{"foo": "bar", "baz": "bar"},
			expPayload: []byte("{\"foo\":\"bar\"}"),
		},
		{
			name:       "basic meta with repeating values",
			payload:    []byte("foo=bar&foo=kaboo {\"foo\":\"bar\"}"),
			expMeta:    map[string]interface{}{"foo": "bar"},
			expPayload: []byte("{\"foo\":\"bar\"}"),
		},
		{
			name:       "basic meta with malformed meta",
			payload:    []byte("foo {\"foo\":\"bar\"}"),
			expMeta:    map[string]interface{}{"foo": ""},
			expPayload: []byte("{\"foo\":\"bar\"}"),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {

			mp, err := New("meta_parser", core.Params{}, core.NewContext())
			if err != nil {
				t.Errorf("Failed to instantiate a meta parser: %s", err)
			}
			testRcv := NewA()
			mp.ConnectTo(testRcv)

			msg := core.NewMessage(testCase.payload)

			sendErr := mp.Recv(msg)

			if sendErr != nil {
				t.Errorf("Failed to send message: %s", sendErr)
			}

			select {
			case <-msg.AckCh():
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Timed out to send message")
			}

			msgMeta := testRcv.lastMsg.MetaAll()
			if !reflect.DeepEqual(msgMeta, testCase.expMeta) {
				t.Errorf("Unexpected message meta: %+v, want: %+v",
					msgMeta, testCase.expMeta)
			}

			if bytes.Compare(testRcv.lastMsg.Payload(), testCase.expPayload) != 0 {
				t.Errorf("Unexpected message payload: %s, want: %s",
					testRcv.lastMsg.Payload(), testCase.expPayload)
			}
		})
	}
}
