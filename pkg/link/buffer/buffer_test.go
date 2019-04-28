package link

import (
	"testing"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/types"
)

func TestBuffer_TestNew(t *testing.T) {
	tests := []struct {
		name     string
		params   types.Params
		expCap   int
		expStr   BufStrategy
		expRetry int
	}{
		{"default params", types.Params{}, 65536, BufStrategySub, 1},
		{"capacity set", types.Params{"capacity": 32768}, 32768, BufStrategySub, 1},
		{"drop strategy set", types.Params{"strategy": "drop"}, 65536, BufStrategyDrop, 1},
		{"block strategy set", types.Params{"strategy": "block"}, 65536, BufStrategyBlock, 1},
		{"sub strategy set", types.Params{"strategy": "sub"}, 65536, BufStrategySub, 1},
		{"max_retry set", types.Params{"max_retry": 5}, 65536, BufStrategySub, 5},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, err := New("buffer", test.params, core.NewContext())
			if err != nil {
				t.Errorf("Failed to initialize buffer: %s", err.Error())
			}
			buf := b.(*Buffer)
			if buf.capacity != test.expCap {
				t.Errorf("Unexpected buf capacity: %d, want: %d", buf.capacity, test.expCap)
			}
			if buf.strategy != test.expStr {
				t.Errorf("Unexpected buf strategy: %d, want: %d", buf.strategy, test.expStr)
			}
			if buf.maxRetry != test.expRetry {
				t.Errorf("Unexpected buf max_retry: %d, want: %d", buf.maxRetry, test.expRetry)
			}
		})
	}
}

func TestBuffer_TestSend(t *testing.T) {

}
