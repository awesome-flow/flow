package cast

import (
	"fmt"

	"github.com/awesome-flow/flow/pkg/types"
)

type CfgMapper struct{}

var _ Mapper = (*CfgMapper)(nil)

func (*CfgMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.Cfg{}
		if components, ok := vmap["components"]; ok {
			res.Components = components.(map[string]types.CfgBlockComponent)
		}
		if pipeline, ok := vmap["pipeline"]; ok {
			res.Pipeline = pipeline.(map[string]types.CfgBlockPipeline)
		}
		if system, ok := vmap["system"]; ok {
			res.System = system.(types.CfgBlockSystem)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgMapper cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockSystemMapper struct{}

var _ Mapper = (*CfgBlockSystemMapper)(nil)

func (*CfgBlockSystemMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockSystem{}
		if maxprocs, ok := vmap["maxprocs"]; ok {
			res.Maxprocs = maxprocs.(int)
		}
		if admin, ok := vmap["admin"]; ok {
			res.Admin = admin.(types.CfgBlockSystemAdmin)
		}
		if metrics, ok := vmap["metrics"]; ok {
			res.Metrics = metrics.(types.CfgBlockSystemMetrics)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockSystem cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockSystemAdminMapper struct{}

var _ Mapper = (*CfgBlockSystemAdminMapper)(nil)

func (*CfgBlockSystemAdminMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockSystemAdmin{}
		if enabled, ok := vmap["enabled"]; ok {
			res.Enabled = enabled.(bool)
		}
		if bindAddr, ok := vmap["bind_addr"]; ok {
			res.BindAddr = bindAddr.(string)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockSystemAdmin cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockSystemMetricsMapper struct{}

var _ Mapper = (*CfgBlockSystemMetricsMapper)(nil)

func (*CfgBlockSystemMetricsMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockSystemMetrics{}
		if enabled, ok := vmap["enabled"]; ok {
			res.Enabled = enabled.(bool)
		}
		if interval, ok := vmap["interval"]; ok {
			res.Interval = interval.(int)
		}
		if receiver, ok := vmap["receiver"]; ok {
			res.Receiver = receiver.(types.CfgBlockSystemMetricsReceiver)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockSystemMetrics cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockSystemMetricsReceiverMapper struct{}

var _ Mapper = (*CfgBlockSystemMetricsReceiverMapper)(nil)

func (*CfgBlockSystemMetricsReceiverMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockSystemMetricsReceiver{}
		if tp, ok := vmap["type"]; ok {
			res.Type = tp.(string)
		}
		if params, ok := vmap["params"]; ok {
			res.Params = params.(map[string]types.Value)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockSystemMetricsReceiver cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type MapCfgBlockComponentMapper struct{}

var _ Mapper = (*MapCfgBlockComponentMapper)(nil)

func (*MapCfgBlockComponentMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := make(map[string]types.CfgBlockComponent)
		for k, v := range vmap {
			res[k] = v.(types.CfgBlockComponent)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("Map[string]CfgBlockComponent cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockComponentMapper struct{}

var _ Mapper = (*CfgBlockComponentMapper)(nil)

func (*CfgBlockComponentMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockComponent{}
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
			res.Params = params.(map[string]types.Value)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockComponent cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type MapCfgBlockPipelineMapper struct{}

var _ Mapper = (*MapCfgBlockComponentMapper)(nil)

func (*MapCfgBlockPipelineMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := make(map[string]types.CfgBlockPipeline)
		for k, v := range vmap {
			res[k] = v.(types.CfgBlockPipeline)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("Map[string]CfgBlockPipeline cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type CfgBlockPipelineMapper struct{}

var _ Mapper = (*CfgBlockPipelineMapper)(nil)

func (*CfgBlockPipelineMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockPipeline{}
		if connect, ok := vmap["connect"]; ok {
			res.Connect = connect.(string)
		}
		if links, ok := vmap["links"]; ok {
			res.Links = links.([]string)
		}
		if routes, ok := vmap["routes"]; ok {
			res.Routes = routes.(map[string]string)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("CfgBlockPipeline cast failed for key: %q, val: %#v", kv.Key.String(), kv.Value)
}

//============================================================================//

type ArrStrMapper struct{}

var _ Mapper = (*ArrStrMapper)(nil)

func (*ArrStrMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	// []interface{}, not []Value because factual arguments are not being
	// type casted
	if arr, ok := kv.Value.([]interface{}); ok {
		res := make([]string, 0, len(arr))
		for _, v := range arr {
			res = append(res, v.(string))
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("[]string cast failed for key: %q, val: %#v", kv.Key, kv.Value)
}

//============================================================================//

type MapStrToStrMapper struct{}

var _ Mapper = (*MapStrToStrMapper)(nil)

func (*MapStrToStrMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if mp, ok := kv.Value.(map[string]types.Value); ok {
		res := make(map[string]string)
		for k, v := range mp {
			res[k] = v.(string)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("map[string]string cast failed for key: %q, val: %#v", kv.Key, kv.Value)
}
