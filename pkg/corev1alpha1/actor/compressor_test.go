package actor

import (
	"fmt"
	"testing"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

func TestNewCompressor(t *testing.T) {

	var NoopCoder = func(b []byte, l int) ([]byte, error) {
		return b, nil
	}

	testCoders := map[string]CoderFunc{
		"test-coder": NoopCoder,
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
			expcoder: NoopCoder,
			explevel: -1,
		},
		{
			name: "level provided",
			params: core.Params{
				"compress": "test-coder",
				"level":    42,
			},
			experr:   nil,
			expcoder: NoopCoder,
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
			ctx, err := newContextWithConfig(map[string]interface{}{})
			if err != nil {
				t.Fatalf("failed to create a context: %s", err)
			}
			compressor, err := NewCompressorWithCoders(name, ctx, testCase.params, testCoders)
			if !eqErr(err, testCase.experr) {
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
