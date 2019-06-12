package cast

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/awesome-flow/flow/pkg/types"
)

func TestCfgMapper(t *testing.T) {
	actors := map[string]types.CfgBlockActor{
		"bar": types.CfgBlockActor{
			Constructor: "constructor",
			Module:      "module",
			Params:      map[string]types.Value{"baz": 42},
			Plugin:      "plugin",
		},
	}
	ppl := map[string]types.CfgBlockPipeline{
		"moo": types.CfgBlockPipeline{
			Connect: "connect",
			Links:   []string{"l1", "l2", "l3"},
			Routes:  map[string]string{"r1": "l1", "r2": "l2", "r3": "l3"},
		},
	}
	sys := types.CfgBlockSystem{
		Admin: types.CfgBlockSystemAdmin{
			BindAddr: "123.45.67.89",
			Enabled:  true,
		},
		Maxprocs: 42,
		Metrics: types.CfgBlockSystemMetrics{
			Enabled:  true,
			Interval: 1e3,
			Receiver: types.CfgBlockSystemMetricsReceiver{
				Params: map[string]types.Value{"p1": 1, "p2": "2", "p3": true},
				Type:   "type",
			},
		},
	}

	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.Cfg{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("Cfg cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"Components defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"actors": actors,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.Cfg{
				Actors: actors,
			}},
			nil,
		},
		{
			"Pipeline defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"pipeline": ppl,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.Cfg{
				Pipeline: ppl,
			}},
			nil,
		},
		{
			"System defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"system": sys,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.Cfg{
				System: sys,
			}},
			nil,
		},
		{
			"Unknown keys defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"unknown1": "foobar",
				"unknown2": "boobar",
			}},
			nil,
			fmt.Errorf("Cfg cast failed for key: %q: unknown attributes: [%s]", types.NewKey("foo"), "unknown1, unknown2"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &CfgMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestCfgBlockSystemMapper(t *testing.T) {
	adm := types.CfgBlockSystemAdmin{
		BindAddr: "123.45.67.89",
		Enabled:  true,
	}
	metrics := types.CfgBlockSystemMetrics{
		Enabled:  true,
		Interval: 1e3,
		Receiver: types.CfgBlockSystemMetricsReceiver{
			Params: map[string]types.Value{"p1": "v1", "p2": "v2", "p3": "v3"},
			Type:   "type",
		},
	}

	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystem{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("CfgBlockSystem cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"Maxprocs defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"maxprocs": 42,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystem{
				Maxprocs: 42,
			}},
			nil,
		},
		{
			"Admin defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"admin": adm,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystem{
				Admin: adm,
			}},
			nil,
		},
		{
			"Metrics defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"metrics": metrics,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystem{
				Metrics: metrics,
			}},
			nil,
		},
		{
			"Unknown keys defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"unknown1": "v1",
				"unknown2": 42,
			}},
			nil,
			fmt.Errorf("CfgBlockSystem cast failed for key: %q: unknown attributes: [%s]", types.NewKey("foo"), "unknown1, unknown2"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &CfgBlockSystemMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestCfgBlockSystemAdminMapper(t *testing.T) {
	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemAdmin{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("CfgBlockSystemAdmin cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"Enabled defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"enabled": true,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemAdmin{
				Enabled: true,
			}},
			nil,
		},
		{
			"BindAddr defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"bind_addr": "123.45.67.89",
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemAdmin{
				BindAddr: "123.45.67.89",
			}},
			nil,
		},
		{
			"Unknown keys defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"unknown1": "v1",
				"unknown2": 42,
			}},
			nil,
			fmt.Errorf("CfgBlockSystemAdmin cast failed for key: %q: unknown attributes: [%s]", types.NewKey("foo"), "unknown1, unknown2"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &CfgBlockSystemAdminMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestCfgBlockSystemMetricsMapper(t *testing.T) {
	rcv := types.CfgBlockSystemMetricsReceiver{
		Params: map[string]types.Value{"p1": "v1", "p2": 2, "p3": true},
		Type:   "type",
	}

	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemMetrics{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("CfgBlockSystemMetrics cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"Enabled defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"enabled": true,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemMetrics{
				Enabled: true,
			}},
			nil,
		},
		{
			"Interval defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"interval": 1,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemMetrics{
				Interval: 1,
			}},
			nil,
		},
		{
			"Receiver defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"receiver": rcv,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemMetrics{
				Receiver: rcv,
			}},
			nil,
		},
		{
			"Unknown keys defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"unknown1": "v1",
				"unknown2": 42,
			}},
			nil,
			fmt.Errorf("CfgBlockSystemMetrics cast failed for key: %q: unknown attributes: [%s]", types.NewKey("foo"), "unknown1, unknown2"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &CfgBlockSystemMetricsMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestCfgBlockSystemMetricsReceiverMapper(t *testing.T) {
	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemMetricsReceiver{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("CfgBlockSystemMetricsReceiver cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"Type defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"type": "type",
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemMetricsReceiver{
				Type: "type",
			}},
			nil,
		},
		{
			"Params defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"params": map[string]types.Value{"p1": "v1", "p2": 2, "p3": true},
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockSystemMetricsReceiver{
				Params: map[string]types.Value{"p1": "v1", "p2": 2, "p3": true},
			}},
			nil,
		},
		{
			"Unknown keys defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"unknown1": "v1",
				"unknown2": 42,
			}},
			nil,
			fmt.Errorf("CfgBlockSystemMetricsReceiver cast failed for key: %q: unknown attributes: [%s]", types.NewKey("foo"), "unknown1, unknown2"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &CfgBlockSystemMetricsReceiverMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestMapCfgBlockActorMapper(t *testing.T) {
	p1 := map[string]types.Value{"p11": 11, "p12": "12"}
	p2 := map[string]types.Value{"p21": 21, "p22": "22"}
	comp1 := types.CfgBlockActor{
		Constructor: "constructor1",
		Module:      "module1",
		Params:      p1,
		Plugin:      "plugin1",
	}
	comp2 := types.CfgBlockActor{
		Constructor: "constructor2",
		Module:      "module2",
		Params:      p2,
		Plugin:      "plugin2",
	}
	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: make(map[string]types.CfgBlockActor)},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("map[string]CfgBlockActor cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"A set of components",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"bar": comp1,
				"baz": comp2,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.CfgBlockActor{
				"bar": comp1,
				"baz": comp2,
			}},
			nil,
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &MapCfgBlockActorMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestCfgBlockActorMapper(t *testing.T) {
	params := map[string]types.Value{"p1": "v1", "p2": 2, "p3": true}

	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockActor{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("CfgBlockActor cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"Constructor defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"constructor": "constructor",
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockActor{
				Constructor: "constructor",
			}},
			nil,
		},
		{
			"Module defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"module": "module",
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockActor{
				Module: "module",
			}},
			nil,
		},
		{
			"Plugin defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"plugin": "plugin",
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockActor{
				Plugin: "plugin",
			}},
			nil,
		},
		{
			"Params defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"params": params,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockActor{
				Params: params,
			}},
			nil,
		},
		{
			"Unknown keys defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"unknown1": "v1",
				"unknown2": 42,
			}},
			nil,
			fmt.Errorf("CfgBlockActor cast failed for key: %q: unknown attributes: [%s]", types.NewKey("foo"), "unknown1, unknown2"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &CfgBlockActorMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestMapCfgBlockPipeline(t *testing.T) {
	ppl1 := types.CfgBlockPipeline{
		Connect: "connect1",
		Links:   []string{"l11", "l12", "l13"},
		Routes:  map[string]string{"r11": "l11", "r12": "l12", "r13": "l13"},
	}
	ppl2 := types.CfgBlockPipeline{
		Connect: "connect2",
		Links:   []string{"l21", "l22", "l23"},
		Routes:  map[string]string{"r21": "l21", "r22": "l22", "r23": "l23"},
	}

	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: make(map[string]types.CfgBlockPipeline)},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("map[string]CfgBlockPipeline cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"A set of components",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"bar": ppl1,
				"baz": ppl2,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.CfgBlockPipeline{
				"bar": ppl1,
				"baz": ppl2,
			}},
			nil,
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &MapCfgBlockPipelineMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestCfgBlockPipelineMapper(t *testing.T) {
	links := []string{"l1", "l2", "l3"}
	routes := map[string]string{"r1": "l1", "r2": "l2", "r3": "l3"}

	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockPipeline{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("CfgBlockPipeline cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"Connect defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"connect": "connect",
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockPipeline{
				Connect: "connect",
			}},
			nil,
		},
		{
			"Links defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"links": links,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockPipeline{
				Links: links,
			}},
			nil,
		},
		{
			"Routes defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"routes": routes,
			}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: types.CfgBlockPipeline{
				Routes: routes,
			}},
			nil,
		},
		{
			"Unknown keys defined",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{
				"unknown1": "v1",
				"unknown2": 42,
			}},
			nil,
			fmt.Errorf("CfgBlockPipeline cast failed for key: %q: unknown attributes: [%s]", types.NewKey("foo"), "unknown1, unknown2"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &CfgBlockPipelineMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestArrStrMapper(t *testing.T) {
	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty list",
			&types.KeyValue{Key: types.NewKey("foo"), Value: []interface{}{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: []string{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("[]string cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"A list",
			&types.KeyValue{Key: types.NewKey("foo"), Value: []interface{}{"foo", "bar", "baz"}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: []string{"foo", "bar", "baz"}},
			nil,
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &ArrStrMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}

func TestMapStrToStrMapper(t *testing.T) {
	tests := []struct {
		name    string
		inputKV *types.KeyValue
		wantKV  *types.KeyValue
		wantErr error
	}{
		{
			"Empty map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]string{}},
			nil,
		},
		{
			"Nil-value",
			&types.KeyValue{Key: types.NewKey("foo"), Value: nil},
			nil,
			fmt.Errorf("map[string]string cast failed for key: %q, val: %#v: unknown value type", types.NewKey("foo"), nil),
		},
		{
			"A map",
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]types.Value{"foo": "bar", "baz": "moo"}},
			&types.KeyValue{Key: types.NewKey("foo"), Value: map[string]string{"foo": "bar", "baz": "moo"}},
			nil,
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := &MapStrToStrMapper{}
			gotKV, gotErr := mpr.Map(testCase.inputKV)
			if !reflect.DeepEqual(gotErr, testCase.wantErr) {
				t.Fatalf("Unexpected error: Map(%#v) = _, %s, want: %s", testCase.inputKV, gotErr, testCase.wantErr)
			}
			if testCase.wantKV != nil && !reflect.DeepEqual(gotKV, testCase.wantKV) {
				t.Fatalf("Unexpected value: Map(%#v) = %#v, want: %#v", testCase.inputKV, gotKV, testCase.wantKV)
			}
		})
	}
}
