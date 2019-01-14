package link

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/DataDog/zstd"

	"github.com/awesome-flow/flow/pkg/core"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

type receiver struct {
	lastmsg *core.Message
	*core.Connector
}

func newReceiver() *receiver {
	return &receiver{
		nil,
		core.NewConnector(),
	}
}

func (rcv *receiver) Recv(msg *core.Message) error {
	rcv.lastmsg = msg
	return msg.AckDone()
}

func compress(payload []byte, format string, level int) []byte {
	var b bytes.Buffer
	var writer io.WriteCloser
	switch format {
	case "gzip":
		var err error
		writer, err = gzip.NewWriterLevel(&b, level)
		if err != nil {
			panic(err.Error())
		}
	case "flate":
		var err error
		writer, err = flate.NewWriter(&b, level)
		if err != nil {
			panic(err.Error())
		}
	case "lzw":
		writer = lzw.NewWriter(&b, lzw.MSB, 8)
	case "zlib":
		var err error
		writer, err = zlib.NewWriterLevel(&b, level)
		if err != nil {
			panic(err.Error())
		}
	case "zstd":
		writer = zstd.NewWriterLevel(&b, level)
	default:
		panic(fmt.Sprintf("unknown compression format: %s", format))
	}
	if _, err := writer.Write(payload); err != nil {
		panic(err.Error())
	}
	writer.Close()

	return b.Bytes()
}

const DefaultMessageSize = 1000 // 1000 chars

func TestCompressGzip(t *testing.T) {

	tests := []struct {
		name    string
		payload []byte
		coder   string
		level   int
	}{
		{
			"gzip empty string",
			[]byte(""),
			"gzip",
			gzip.DefaultCompression,
		},
		{
			"gzip non-empty string",
			[]byte(testutil.RandStringBytes(DefaultMessageSize)),
			"gzip",
			gzip.DefaultCompression,
		},
		{
			"flate empty string",
			[]byte{},
			"flate",
			flate.DefaultCompression,
		},
		{
			"flage non-empty string",
			[]byte(testutil.RandStringBytes(DefaultMessageSize)),
			"flate",
			flate.DefaultCompression,
		},
		{
			"lzw empty string",
			[]byte{},
			"lzw",
			-1,
		},
		{
			"lzw non-empty string",
			[]byte(testutil.RandStringBytes(DefaultMessageSize)),
			"lzw",
			-1,
		},
		{
			"zlib empty string",
			[]byte{},
			"zlib",
			flate.DefaultCompression,
		},
		{
			"zlib non-empty string",
			[]byte(testutil.RandStringBytes(DefaultMessageSize)),
			"zlib",
			flate.DefaultCompression,
		},
		{
			"zstd empty string",
			[]byte{},
			"zstd",
			1,
		},
		{
			"zstd non-empty string",
			[]byte(testutil.RandStringBytes(DefaultMessageSize)),
			"zstd",
			1,
		},
	}

	t.Parallel()

	for _, testcase := range tests {
		t.Run(testcase.name, func(t *testing.T) {
			cmp, err := New(
				"compressor",
				core.Params{"algo": testcase.coder},
				core.NewContext(),
			)
			if err != nil {
				t.Fatalf(err.Error())
			}

			rcv := newReceiver()
			cmp.ConnectTo(rcv)
			msg := core.NewMessage([]byte(testcase.payload))
			if err := cmp.Recv(msg); err != nil {
				t.Fatalf(err.Error())
			}
			<-msg.GetAckCh()

			if rcv.lastmsg == nil {
				t.Fatalf("empty message in the receiver")
			}

			expected := compress([]byte(testcase.payload), testcase.coder, testcase.level)
			if !reflect.DeepEqual(rcv.lastmsg.Payload, expected) {
				t.Fatalf("payload mismatch: got: %s, want: %s", rcv.lastmsg.Payload, expected)
			}
		})
	}
}
