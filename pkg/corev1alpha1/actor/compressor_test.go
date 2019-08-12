package actor

import (
	"fmt"
	"math/rand"
	"testing"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	coretest "github.com/awesome-flow/flow/pkg/corev1alpha1/test"
	"github.com/awesome-flow/flow/pkg/util"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

func TestNewCompressor(t *testing.T) {

	var noopCoder = func(b []byte, l int) ([]byte, error) {
		return b, nil
	}

	testCoders := map[string]CoderFunc{
		"test-coder": noopCoder,
	}

	name := "test-compressor"

	tests := []struct {
		name     string
		params   core.Params
		experr   error
		expcoder CoderFunc
		explevel int
	}{
		{
			name:   "missing compress config",
			params: core.Params{},
			experr: fmt.Errorf("compressor %q is missing `compress` config", name),
		},
		{
			name: "unknown compress config",
			params: core.Params{
				"compress": "unknown-coder",
			},
			experr: fmt.Errorf("compressor %q: unknown compression algorithm %q", name, "unknown-coder"),
		},
		{
			name: "no level provided",
			params: core.Params{
				"compress": "test-coder",
			},
			experr:   nil,
			expcoder: noopCoder,
			explevel: -1,
		},
		{
			name: "level provided",
			params: core.Params{
				"compress": "test-coder",
				"level":    42,
			},
			experr:   nil,
			expcoder: noopCoder,
			explevel: 42,
		},
		{
			name: "malformed level provided",
			params: core.Params{
				"compress": "test-coder",
				"level":    "asdf",
			},
			experr: fmt.Errorf("compressor %q: malformed compression level provided: got: %+v, want: an integer", name, "asdf"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, err := coretest.NewContextWithConfig(map[string]interface{}{})
			if err != nil {
				t.Fatalf("failed to create a context: %s", err)
			}

			if err := ctx.Start(); err != nil {
				t.Fatalf("failed to start context: %s", err)
			}
			defer ctx.Stop()

			compressor, err := NewCompressorWithCoders(name, ctx, testCase.params, testCoders)
			if !coretest.EqErr(err, testCase.experr) {
				t.Fatalf("unexpected error: got: %q, want: %q", err, testCase.experr)
			}
			if err != nil {
				return
			}

			// functions are not addressable in Go, and this
			// comparison is a super dummy way of getting func
			// addresses. Better than nothing.
			if fmt.Sprintf("%+v", compressor.(*Compressor).coder) != fmt.Sprintf("%+v", testCase.expcoder) {
				t.Fatalf("unpexpected coder selected: got: %+v, want: %+v", compressor.(*Compressor).coder, testCase.expcoder)
			}
			if compressor.(*Compressor).level != testCase.explevel {
				t.Fatalf("unexpected compression level: got: %d, want: %d", compressor.(*Compressor).level, testCase.explevel)
			}
		})
	}
}

func TestCompressorReceive(t *testing.T) {
	var workingCoder = func(b []byte, l int) ([]byte, error) {
		return b, nil
	}

	var brokenCoder = func(b []byte, l int) ([]byte, error) {
		return nil, fmt.Errorf("failed to encode")
	}

	testCoders := map[string]CoderFunc{
		"working-coder": workingCoder,
		"broken-coder":  brokenCoder,
	}

	name := "test-compressor"

	tests := []struct {
		name      string
		codername string
		experr    error
		expstatus core.MsgStatus
	}{
		{
			name:      "working coder",
			codername: "working-coder",
			experr:    nil,
			expstatus: core.MsgStatusDone,
		},
		{
			name:      "broken coder",
			codername: "broken-coder",
			experr:    fmt.Errorf("failed to encode"),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, err := coretest.NewContextWithConfig(map[string]interface{}{
				"system.maxprocs": rand.Intn(4) + 1,
			})
			if err != nil {
				t.Fatalf("failed to create a context: %s", err)
			}

			if err := ctx.Start(); err != nil {
				t.Fatalf("failed to start context: %s", err)
			}

			params := core.Params{
				"compress": testCase.codername,
			}

			compressor, err := NewCompressorWithCoders(name, ctx, params, testCoders)

			act, err := flowtest.NewTestActor("test-actor", ctx, core.Params{})
			if err != nil {
				t.Fatalf("failed to initialize test actor: %s", err)
			}

			act.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
				msg.Complete(core.MsgStatusDone)
				act.(*flowtest.TestActor).Flush()
			})

			if err := compressor.Connect(rand.Intn(4)+1, act); err != nil {
				t.Fatalf("failed to connect compressor and test actor: %s", err)
			}

			if err := util.ExecEnsure(
				act.Start,
				compressor.Start,
			); err != nil {
				t.Fatalf("failed to start actors: %s", err)
			}
			defer util.ExecEnsure(
				compressor.Stop,
				act.Stop,
			)

			msg := core.NewMessage(testutil.RandBytes(1024))
			err = compressor.Receive(msg)
			if !eqErr(err, testCase.experr) {
				t.Fatalf("unexpected error: got: %s, want: %s", err, testCase.experr)
			}

			if err != nil {
				return
			}

			if s := msg.Await(); s != testCase.expstatus {
				t.Fatalf("unexpected message status: got: %d, want: %d", s, testCase.expstatus)
			}
		})
	}
}
