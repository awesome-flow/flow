package corev1alpha1

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func TestLogger(t *testing.T) {
	out := new(bytes.Buffer)
	l := NewLogger(out)
	if err := l.Start(); err != nil {
		t.Fatalf("failed to start logger")
	}
	sever := []LogSev{
		LogSevDebug,
		LogSevTrace,
		LogSevInfo,
		LogSevWarn,
		LogSevError,
	}
	funcs := map[LogSev]func(string, ...interface{}){
		LogSevDebug: l.Debug,
		LogSevTrace: l.Trace,
		LogSevInfo:  l.Info,
		LogSevWarn:  l.Warn,
		LogSevError: l.Error,
	}
	res := make([]string, 0, 1024)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < cap(res); i++ {
		ix := rand.Intn(len(sever))
		logmsg := testutil.RandBytes(testutil.RandInt(1024))
		res = append(res, fmt.Sprintf("%s\t%s", LogSevLex[sever[ix]], logmsg))
		funcs[sever[ix]](string(logmsg))
	}

	if err := l.Stop(); err != nil {
		t.Fatalf("failed to stop logger: %s", err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(out.Bytes()))

	ix := 0
	for scanner.Scan() {
		logline := scanner.Text()
		if !strings.HasSuffix(logline, res[ix]) {
			t.Fatalf("logline %q is expected to contain suffix %q", logline, res[ix])
		}
		ix++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner returned an error: %s", err)
	}
	if ix != len(res) {
		t.Fatalf("output is incomplete: got: %d lines, want: %d lines", ix, len(res))
	}
}
