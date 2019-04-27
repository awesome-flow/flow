package cast

import "fmt"

type Cfg struct {
	Components map[string]CfgBlockComponent
	Pipeline   map[string]CfgBlockPipeline
	System     CfgBlockSystem
}

type CfgBlockSystem struct {
	Admin    CfgBlockSystemAdmin
	Maxprocs int
	Metrics  CfgBlockSystemMetrics
}

type CfgBlockSystemAdmin struct {
	BindAddr string
	Enabled  bool
}

type CfgBlockSystemMetrics struct {
	Enabled  bool
	Interval int
	Receiver CfgBlockSystemMetricsReceiver
}

type CfgBlockSystemMetricsReceiver struct {
	Params map[string]Value
	Type   string
}

type CfgBlockComponent struct {
	Constructor string
	Module      string
	Params      map[string]Value
	Plugin      string
}

type CfgBlockPipeline struct {
	Connect string
	Links   []string
	Routes  map[string]string
}

//============================================================================//

type CfgMapper struct{}

var _ Mapper = (*CfgMapper)(nil)

func (*CfgMapper) Map(kv *KeyValue) (*KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]Value); ok {
		res := Cfg{}
		if components, ok := vmap["components"]; ok {
			res.Components = components.(map[string]CfgBlockComponent)
		}
		if pipeline, ok := vmap["pipeline"]; ok {
			res.Pipeline = pipeline.(map[string]CfgBlockPipeline)
		}
		if system, ok := vmap["system"]; ok {
			res.System = system.(CfgBlockSystem)
		}
		return &KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgMapper cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockSystemMapper struct{}

var _ Mapper = (*CfgBlockSystemMapper)(nil)

func (*CfgBlockSystemMapper) Map(kv *KeyValue) (*KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]Value); ok {
		res := CfgBlockSystem{}
		if maxprocs, ok := vmap["maxprocs"]; ok {
			res.Maxprocs = maxprocs.(int)
		}
		if admin, ok := vmap["admin"]; ok {
			res.Admin = admin.(CfgBlockSystemAdmin)
		}
		if metrics, ok := vmap["metrics"]; ok {
			res.Metrics = metrics.(CfgBlockSystemMetrics)
		}
		return &KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockSystem cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockSystemAdminMapper struct{}

var _ Mapper = (*CfgBlockSystemAdminMapper)(nil)

func (*CfgBlockSystemAdminMapper) Map(kv *KeyValue) (*KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]Value); ok {
		res := CfgBlockSystemAdmin{}
		if enabled, ok := vmap["enabled"]; ok {
			res.Enabled = enabled.(bool)
		}
		if bindAddr, ok := vmap["bind_addr"]; ok {
			res.BindAddr = bindAddr.(string)
		}
		return &KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockSystemAdmin cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockSystemMetricsMapper struct{}

var _ Mapper = (*CfgBlockSystemMetricsMapper)(nil)

func (*CfgBlockSystemMetricsMapper) Map(kv *KeyValue) (*KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]Value); ok {
		res := CfgBlockSystemMetrics{}
		if enabled, ok := vmap["enabled"]; ok {
			res.Enabled = enabled.(bool)
		}
		if interval, ok := vmap["interval"]; ok {
			res.Interval = interval.(int)
		}
		if receiver, ok := vmap["receiver"]; ok {
			res.Receiver = receiver.(CfgBlockSystemMetricsReceiver)
		}
		return &KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockSystemMetrics cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockSystemMetricsReceiverMapper struct{}

var _ Mapper = (*CfgBlockSystemMetricsReceiverMapper)(nil)

func (*CfgBlockSystemMetricsReceiverMapper) Map(kv *KeyValue) (*KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]Value); ok {
		res := CfgBlockSystemMetricsReceiver{}
		if tp, ok := vmap["type"]; ok {
			res.Type = tp.(string)
		}
		if params, ok := vmap["params"]; ok {
			res.Params = params.(map[string]Value)
		}
		return &KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockSystemMetricsReceiver cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type MapCfgBlockComponentMapper struct{}

var _ Mapper = (*MapCfgBlockComponentMapper)(nil)

func (*MapCfgBlockComponentMapper) Map(kv *KeyValue) (*KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]Value); ok {
		res := make(map[string]CfgBlockComponent)
		for k, v := range vmap {
			res[k] = v.(CfgBlockComponent)
		}
		return &KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("Map[string]CfgBlockComponent cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockComponentMapper struct{}

var _ Mapper = (*CfgBlockComponentMapper)(nil)

func (*CfgBlockComponentMapper) Map(kv *KeyValue) (*KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]Value); ok {
		res := CfgBlockComponent{}
		if constructor, ok := vmap["constructor"]; ok {
			res.Constructor = constructor.(string)
		}
		if module, ok := vmap["module"]; ok {
			res.Module = module.(string)
		}
		if plugin, ok := vmap["plugin"]; ok {
			res.Plugin = plugin.(string)
		}
		if params, ok := vmap["params"]; ok {
			res.Params = params.(map[string]Value)
		}
		return &KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockComponent cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//
