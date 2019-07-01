package corev1alpha1

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func TestNewMessage(t *testing.T) {
	body := testutil.RandBytes(1024)
	msg := NewMessage(body)
	if !reflect.DeepEqual(msg.body, body) {
		t.Fatalf("unexpected message body: got: %s, want: %s", msg.body, body)
	}
	if msg.status != MsgStatusNew {
		t.Fatalf("unexpected message status: got %d, want: %d", msg.status, MsgStatusNew)
	}
}

func TestNewMessageCopyBody(t *testing.T) {
	body := testutil.RandBytes(1024)
	msg := NewMessage(body)
	// RandBytes guarantees to place a non-zero byte at any pos
	body[rand.Intn(len(body))] = '\x00'
	if reflect.DeepEqual(msg.body, body) {
		t.Fatalf("detected a side affect of the original body modification")
	}
}

func TestAwait(t *testing.T) {
	done := make(chan struct{})
	msg := NewMessage(testutil.RandBytes(1024))
	go func() {
		msg.Await()
		close(done)
	}()
	msg.Complete(MsgStatusDone)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out to await")
	}
}

func TestAwaitChan(t *testing.T) {
	msg := NewMessage(testutil.RandBytes(1024))
	go func() {
		msg.Complete(MsgStatusDone)
	}()
	select {
	case <-msg.AwaitChan():
	case <-time.After(time.Second):
		t.Fatalf("timed out to await chan")
	}
}

func TestMetaKeys(t *testing.T) {
	msg := NewMessage(testutil.RandBytes(1024))
	meta := make(map[string]interface{})
	for i, max := 0, testutil.RandInt(128); i < max; i++ {
		k := string(testutil.RandBytes(1024))
		v := testutil.RandBytes(1024)
		meta[k] = v
		msg.SetMeta(k, v)
	}
	for k, v := range meta {
		if msgv, _ := msg.Meta(k); !reflect.DeepEqual(msgv, v) {
			t.Fatalf("unexpected value in msg meta: %q, want: %q", msgv, v)
		}
	}
}

func TestCopy(t *testing.T) {
	msg := NewMessage(testutil.RandBytes(1024))
	for i, max := 0, testutil.RandInt(128); i < max; i++ {
		k := string(testutil.RandBytes(1024))
		v := testutil.RandBytes(1024)
		msg.SetMeta(k, v)
	}
	cpmsg := msg.Copy()
	if !reflect.DeepEqual(msg.Body(), cpmsg.Body()) {
		t.Fatalf("unexpected message body: %q, want: %q", cpmsg.Body(), msg.Body())
	}
	if !reflect.DeepEqual(cpmsg.meta, msg.meta) {
		t.Fatalf("unexpected message meta: %v, want: %v", cpmsg.meta, msg.meta)
	}
}
