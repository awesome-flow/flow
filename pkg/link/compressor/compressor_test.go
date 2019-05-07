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
	"github.com/golang/snappy"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util/core_test"
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
	case "snappy":
		writer = snappy.NewBufferedWriter(&b)
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
		{
			"snappy empty string",
			[]byte{},
			"snappy",
			1,
		},
		{
			"snappy non-empty string",
			[]byte(testutil.RandStringBytes(DefaultMessageSize)),
			"snappy",
			1,
		},
	}

	t.Parallel()

	for _, testcase := range tests {
		t.Run(testcase.name, func(t *testing.T) {
			cmp, err := New(
				"compressor",
				types.Params{"algo": testcase.coder},
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
			<-msg.AckCh()

			if rcv.lastmsg == nil {
				t.Fatalf("empty message in the receiver")
			}

			expected := compress([]byte(testcase.payload), testcase.coder, testcase.level)
			if !reflect.DeepEqual(rcv.lastmsg.Payload(), expected) {
				t.Fatalf("payload mismatch: got: %s, want: %s", rcv.lastmsg.Payload(), expected)
			}
		})
	}
}

func BenchmarkCompressorGzipBestSpeed(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "gzip", "level": gzip.BestSpeed}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over gzip link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressorGzipBestCompression(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "gzip", "level": gzip.BestCompression}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over gzip link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressorFlateBestSpeed(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "flate", "level": flate.BestSpeed}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over flate link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressorFlateBestCompression(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "flate", "level": flate.BestCompression}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over flate link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressorLZW(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "lzw"}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over flate link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressionZLIBestSpeed(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "zlib", "level": flate.BestSpeed}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over flate link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressionZLIBestCompression(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "zlib", "level": flate.BestCompression}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over flate link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressionZSTDBestSpeed(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "zstd", "level": 1}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over flate link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressionZSTDBestCompression(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "zstd", "level": 19}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over flate link")
		}
		<-msg.AckCh()
	}
}

func BenchmarkCompressionSnappy(b *testing.B) {
	rcv := core_test.NewCountAndReply("counter", core_test.ReplyDone)
	cmp, err := New("compressor", types.Params{"algo": "snappy"}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	cmp.ConnectTo(rcv)
	payload := []byte(testutil.RandStringBytes(1024))
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(payload)
		if err := rcv.Recv(msg); err != nil {
			panic("Error on sending over flate link")
		}
		<-msg.AckCh()
	}
}
