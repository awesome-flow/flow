package actor

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

type testWriteCloser struct {
	bytes.Buffer
}

var _ (io.WriteCloser) = (*testWriteCloser)(nil)

func NewTestWriteCloser() io.WriteCloser {
	return &testWriteCloser{}
}

func (wc *testWriteCloser) Close() error {
	return nil
}

func TestSinkHeadFileConnect(t *testing.T) {
	out := NewTestWriteCloser()
	opener := func(string) (io.WriteCloser, error) {
		return out, nil
	}
	head, err := NewSinkHeadFile("/dev/null")
	if err != nil {
		t.Fatalf("failed to create file sink head: %s", err)
	}
	head.Opener = opener
	if err := head.Connect(); err != nil {
		t.Fatalf("failed to connect file sink head: %s", err)
	}
	if !reflect.DeepEqual(head.out, out) {
		t.Fatalf("unexpected head out: got: %+v, want: %+v", head.out, out)
	}
}

func TestSinkHeadFileWrite(t *testing.T) {
	out := NewTestWriteCloser()
	opener := func(string) (io.WriteCloser, error) {
		return out, nil
	}
	head, err := NewSinkHeadFile("/dev/null")
	if err != nil {
		t.Fatalf("failed to create file sink head: %s", err)
	}
	head.Opener = opener
	if err := head.Connect(); err != nil {
		t.Fatalf("failed to connect file sink head: %s", err)
	}
	data := testutil.RandBytes(1024)
	n, err, rec := head.Write(data)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n != len(data)+2 {
		t.Fatalf("unexpected write bytes length: got: %d, want: %d", n, len(data)+2)
	}
	if rec {
		t.Fatalf("unexpected reconnect flag: got: %t, want: false", rec)
	}
	gotdata := out.(*testWriteCloser).Bytes()
	wantdata := append(data, []byte("\r\n")...)
	if !reflect.DeepEqual(gotdata, wantdata) {
		t.Fatalf("unexpected buf contents: got: %q, want: %q", gotdata, wantdata)
	}
}
