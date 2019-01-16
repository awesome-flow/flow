package core

import (
	"math"
	"reflect"
	"testing"
	"time"
)

type resFound struct {
	key   string
	res   interface{}
	found bool
}

func TestMessage_NewMessage(t *testing.T) {
	msg := NewMessage([]byte{})
	var v interface{}
	var ok bool
	v, ok = msg.Meta("foo")
	if ok {
		t.Errorf("Unexpected presence flag for key foo")
	}
	if v != nil {
		t.Errorf("Unexpected return value from en empty meta")
	}
	v, ok = msg.MetaOrDefault("foo", "bar")
	if ok {
		t.Errorf("Unexpected presence flag for key foo")
	}
	if v != "bar" {
		t.Errorf("Unexpected default value for key foo")
	}

	metaAll := msg.MetaAll()
	if !reflect.DeepEqual(metaAll, map[string]interface{}{}) {
		t.Errorf("Unexpected contents of message meta: %+v, ecpected: an empty map",
			metaAll)
	}

	msg.SetMeta("foo", "bar")
	metaAll = msg.MetaAll()
	expMeta := map[string]interface{}{"foo": "bar"}
	if !reflect.DeepEqual(metaAll, expMeta) {
		t.Errorf("Unexpected contents of message meta: %+v, ecpected: %+v",
			metaAll, expMeta)
	}

	msg.SetMetaAll(map[string]interface{}{"foo": "baz", "baz": "bar"})
	metaAll = msg.MetaAll()
	expMeta = map[string]interface{}{"foo": "baz", "baz": "bar"}
	if !reflect.DeepEqual(metaAll, expMeta) {
		t.Errorf("Unexpected contents of message meta: %+v, ecpected: %+v",
			metaAll, expMeta)
	}

	v, ok = msg.MetaOrDefault("baz", "boooo")
	if !ok {
		t.Errorf("Unexpected presence flag for key baz")
	}
	if v != "bar" {
		t.Errorf("Unexpected value for key baz")
	}

	msg.UnsetMeta("foo")

	metaAll = msg.MetaAll()
	expMeta = map[string]interface{}{"baz": "bar"}
	if !reflect.DeepEqual(metaAll, expMeta) {
		t.Errorf("Unexpected contents of message meta: %+v, ecpected: %+v",
			metaAll, expMeta)
	}
}

func TestMessage_GetMeta(t *testing.T) {
	tests := []struct {
		name     string
		meta     map[string]interface{}
		expected []resFound
	}{
		{
			name: "plain meta",
			meta: map[string]interface{}{
				"foo": "bar",
				"bar": "baz",
			},
			expected: []resFound{
				{"foo", "bar", true},
				{"bar", "baz", true},
				{"", "", false},
				{"foobar", "", false},
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			msg := NewMessageWithMeta(testCase.meta, []byte{})
			for _, exp := range testCase.expected {
				v, ok := msg.Meta(exp.key)
				if exp.found != ok {
					t.Errorf("Found flag does not match: %t, want: %t", ok, exp.found)
				}
				if exp.found {
					if v.(string) != exp.res.(string) {
						t.Errorf("Unexpected val for key %s: %s, want: %s",
							exp.key, v, exp.res)
					}
				}
			}
		})
	}
}

func TestMessage_GetMetaAll(t *testing.T) {
	tests := []struct {
		name         string
		meta         map[string]interface{}
		expectedMeta map[string]interface{}
	}{
		{
			"regular map",
			map[string]interface{}{
				"foo": "bar",
			},
			map[string]interface{}{
				"foo": "bar",
			},
		},
		{
			"empty map",
			map[string]interface{}{},
			map[string]interface{}{},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			msg := NewMessageWithMeta(testCase.meta, []byte{})
			msgMeta := msg.MetaAll()
			if !reflect.DeepEqual(msgMeta, testCase.expectedMeta) {
				t.Errorf("Unexpected message meta: %+v, want: %+v",
					msgMeta, testCase.expectedMeta)
			}
		})
	}
}

func TestMessage_Ack(t *testing.T) {
	NoStatus := uint8(math.MaxUint8)
	tests := []struct {
		name      string
		funcName  string
		expErr    error
		expMsgSts MsgStatus
	}{
		{"done", "AckDone", nil, MsgStatusDone},
		{"continue", "AckContinue", nil, NoStatus},
		{"invalid", "AckInvalid", ErrMsgInvalid, MsgStatusInvalid},
		{"partial", "AckPartialSend", ErrMsgPartialSend, MsgStatusPartialSend},
		{"failed", "AckFailed", ErrMsgFailed, MsgStatusFailed},
		{"timeout", "AckTimedOut", ErrMsgTimedOut, MsgStatusTimedOut},
		{"unroutable", "AckUnroutable", ErrMsgUnroutable, MsgStatusUnroutable},
		{"throttled", "AckThrottled", ErrMsgThrottled, MsgStatusThrottled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMessage([]byte(""))
			ret := reflect.ValueOf(msg).MethodByName(tt.funcName).Call([]reflect.Value{})
			err := ret[0].Interface()
			if !reflect.DeepEqual(err, tt.expErr) {
				t.Errorf("Unexpected error code: %s", err)
			}
			if tt.expMsgSts == NoStatus {
				return
			}
			select {
			case s := <-msg.ackCh:
				if s != tt.expMsgSts {
					t.Errorf("Unexpected msg status: %d", s)
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("Timed out to receive a signal from msg ack chan")
			}
		})
	}
}

func TestMessage_NewMessageWithAckCh(t *testing.T) {
	ch := make(chan MsgStatus, 1)
	msg := NewMessageWithAckCh(ch, nil, []byte(""))
	msg.AckDone()
	select {
	case s := <-ch:
		if s != MsgStatusDone {
			t.Errorf("Unexpected message status: %d", s)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out to receive a signal from channel")
	}
}

func TestMessage_CpMessage(t *testing.T) {
	pl1 := []byte("payload1")
	meta1 := map[string]interface{}{"k1": "v1"}
	msg1 := NewMessageWithMeta(meta1, pl1)
	msg2 := CpMessage(msg1)
	(msg2.Payload())[len(msg2.Payload())-1] = '2'
	if !reflect.DeepEqual(msg2.Payload(), []byte("payload2")) {
		t.Errorf("Unexpected payload in msg2: %s", msg2.Payload())
	}
	if !reflect.DeepEqual(msg1.Payload(), pl1) {
		t.Errorf("Unexpected payload in msg1: %s", msg1.Payload())
	}
}

func TestMessage_BumpAttempts(t *testing.T) {
	msg := NewMessage([]byte(""))
	if msg.attempts != 0 {
		t.Errorf("Unexpected msg attempts: %d, want: %d", msg.attempts, 0)
	}
	if err := msg.BumpAttempts(); err != nil {
		t.Errorf("Unexpected error while bumping the message attempts cntr: %s", err.Error())
	}
	if msg.attempts != 1 {
		t.Errorf("Unexpected msg attempts counter: %d, want: %d", msg.attempts, 1)
	}
}
