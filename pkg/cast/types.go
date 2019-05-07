package cast

import (
	"fmt"
	"sort"
	"strings"

	"github.com/awesome-flow/flow/pkg/types"
)

// CfgMapper is the root-level system config encorporating the primary config
// blocks, as: system, components and pipeline.
type CfgMapper struct{}

var _ Mapper = (*CfgMapper)(nil)

func errUnknownValType(castType string, kv *types.KeyValue) error {
	return fmt.Errorf("%s cast failed for key: %q, val: %#v: unknown value type", castType, kv.Key, kv.Value)
}

func errUnknownKeys(castType string, kv *types.KeyValue, unknown map[string]struct{}) error {
	unknownArr := make([]string, 0, len(unknown))
	for k := range unknown {
		unknownArr = append(unknownArr, k)
	}
	sort.Strings(unknownArr)
	return fmt.Errorf("%s cast failed for key: %q: unknown attributes: [%s]", castType, kv.Key, strings.Join(unknownArr, ", "))
}

// Map converts map[string]Value to types.Cfg{} structure.
// Lookup keys are:
// * components
// * pipeline
// * system
// No extra keys are allowed under this section.
func (*CfgMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	var resKV *types.KeyValue
	var err error
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.Cfg{}
		keys := make(map[string]struct{})
		for k := range vmap {
			keys[k] = struct{}{}
		}
		if components, ok := vmap["components"]; ok {
			delete(keys, "components")
			res.Components = components.(map[string]types.CfgBlockComponent)
		}
		if pipeline, ok := vmap["pipeline"]; ok {
			delete(keys, "pipeline")
			res.Pipeline = pipeline.(map[string]types.CfgBlockPipeline)
		}
		if system, ok := vmap["system"]; ok {
			delete(keys, "system")
			res.System = system.(types.CfgBlockSystem)
		}
		if len(keys) > 0 {
			err = errUnknownKeys("Cfg", kv, keys)
		} else {
			resKV = &types.KeyValue{Key: kv.Key, Value: res}
		}
	} else {
		err = errUnknownValType("Cfg", kv)
	}
	if err != nil {
		return nil, err
	}
	return resKV, nil
}

//============================================================================//

// CfgBlockSystemMapper represents the system config section.
type CfgBlockSystemMapper struct{}

var _ Mapper = (*CfgBlockSystemMapper)(nil)

// Map converts map[string]Value to types.CfgBlockSystem{} structure.
// Lookup keys are:
// * maxprocs
// * admin
// * metrics
// No extra keys are allowed under this section.
func (*CfgBlockSystemMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	var resKV *types.KeyValue
	var err error
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockSystem{}
		keys := make(map[string]struct{})
		for k := range vmap {
			keys[k] = struct{}{}
		}
		if maxprocs, ok := vmap["maxprocs"]; ok {
			delete(keys, "maxprocs")
			res.Maxprocs = maxprocs.(int)
		}
		if admin, ok := vmap["admin"]; ok {
			delete(keys, "admin")
			res.Admin = admin.(types.CfgBlockSystemAdmin)
		}
		if metrics, ok := vmap["metrics"]; ok {
			delete(keys, "metrics")
			res.Metrics = metrics.(types.CfgBlockSystemMetrics)
		}
		if len(keys) > 0 {
			err = errUnknownKeys("CfgBlockSystem", kv, keys)
		} else {
			resKV = &types.KeyValue{Key: kv.Key, Value: res}
		}
	} else {
		err = errUnknownValType("CfgBlockSystem", kv)
	}
	if err != nil {
		return nil, err
	}
	return resKV, nil
}

//============================================================================//

// CfgBlockSystemAdminMapper represents a mapper for system.admin config section.
type CfgBlockSystemAdminMapper struct{}

var _ Mapper = (*CfgBlockSystemAdminMapper)(nil)

// Map converts map[string]Value to types.CfgBlockSystemAdmin{} structure.
// Lookup keys are:
// * enabled
// * bind_addr
// No extra keys are allowed under this section.
func (*CfgBlockSystemAdminMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	var resKV *types.KeyValue
	var err error
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockSystemAdmin{}
		keys := make(map[string]struct{})
		for k := range vmap {
			keys[k] = struct{}{}
		}
		if enabled, ok := vmap["enabled"]; ok {
			delete(keys, "enabled")
			res.Enabled = enabled.(bool)
		}
		if bindAddr, ok := vmap["bind_addr"]; ok {
			delete(keys, "bind_addr")
			res.BindAddr = bindAddr.(string)
		}
		if len(keys) > 0 {
			err = errUnknownKeys("CfgBlockSystemAdmin", kv, keys)
		} else {
			resKV = &types.KeyValue{Key: kv.Key, Value: res}
		}
	} else {
		err = errUnknownValType("CfgBlockSystemAdmin", kv)
	}
	if err != nil {
		return nil, err
	}
	return resKV, nil
}

//============================================================================//

// CfgBlockSystemMetricsMapper represents a mapper for system.metrics section.
type CfgBlockSystemMetricsMapper struct{}

var _ Mapper = (*CfgBlockSystemMetricsMapper)(nil)

// Map converts map[string]Value to types.CfgBlockSystemMetrics{} structure.
// Lookup keys are:
// * enabled
// * interval
// * receiver
// No extra keys are allowed in this section.
func (*CfgBlockSystemMetricsMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	var resKV *types.KeyValue
	var err error
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockSystemMetrics{}
		keys := make(map[string]struct{})
		for k := range vmap {
			keys[k] = struct{}{}
		}
		if enabled, ok := vmap["enabled"]; ok {
			delete(keys, "enabled")
			res.Enabled = enabled.(bool)
		}
		if interval, ok := vmap["interval"]; ok {
			delete(keys, "interval")
			res.Interval = interval.(int)
		}
		if receiver, ok := vmap["receiver"]; ok {
			delete(keys, "receiver")
			res.Receiver = receiver.(types.CfgBlockSystemMetricsReceiver)
		}
		if len(keys) > 0 {
			err = errUnknownKeys("CfgBlockSystemMetrics", kv, keys)
		} else {
			resKV = &types.KeyValue{Key: kv.Key, Value: res}
		}
	} else {
		err = errUnknownValType("CfgBlockSystemMetrics", kv)
	}
	if err != nil {
		return nil, err
	}
	return resKV, nil
}

//============================================================================//

// CfgBlockSystemMetricsReceiverMapper represents a mapper for
// system.metrics.receiver section.
type CfgBlockSystemMetricsReceiverMapper struct{}

var _ Mapper = (*CfgBlockSystemMetricsReceiverMapper)(nil)

// Map convers map[string]Value to types.CfgBlockSystemMetricsReceiver{} structure.
// Lookup keys are:
// * type
// * params
// No extra keys are allowed in this section.
func (*CfgBlockSystemMetricsReceiverMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	var resKV *types.KeyValue
	var err error
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockSystemMetricsReceiver{}
		keys := make(map[string]struct{})
		for k := range vmap {
			keys[k] = struct{}{}
		}
		if tp, ok := vmap["type"]; ok {
			delete(keys, "type")
			res.Type = tp.(string)
		}
		if params, ok := vmap["params"]; ok {
			delete(keys, "params")
			res.Params = params.(map[string]types.Value)
		}
		if len(keys) > 0 {
			err = errUnknownKeys("CfgBlockSystemMetricsReceiver", kv, keys)
		} else {
			resKV = &types.KeyValue{Key: kv.Key, Value: res}
		}
	} else {
		err = errUnknownValType("CfgBlockSystemMetricsReceiver", kv)
	}
	if err != nil {
		return nil, err
	}
	return resKV, nil
}

//============================================================================//

// MapCfgBlockComponentMapper represents a mapper for components config section.
type MapCfgBlockComponentMapper struct{}

var _ Mapper = (*MapCfgBlockComponentMapper)(nil)

// Map converts map[string]Value to map[string]types.CfgBlockComponent.
// All keys from the original map will be kept in the resulting structure.
func (*MapCfgBlockComponentMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := make(map[string]types.CfgBlockComponent)
		for k, v := range vmap {
			res[k] = v.(types.CfgBlockComponent)
		}
		return &types.KeyValue{Key: kv.Key, Value: res}, nil
	}
	return nil, errUnknownValType("map[string]CfgBlockComponent", kv)
}

//============================================================================//

// CfgBlockComponentMapper represents a mapper for components.* config section.
type CfgBlockComponentMapper struct{}

var _ Mapper = (*CfgBlockComponentMapper)(nil)

// Map converts map[string]Value to types.CfgBlockComponent{}.
// Lookup keys are:
// * constructor
// * module
// * plugin
// * params
// No extra keys are allowed in this section.
func (*CfgBlockComponentMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	var resKV *types.KeyValue
	var err error
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockComponent{}
		keys := make(map[string]struct{})
		for k := range vmap {
			keys[k] = struct{}{}
		}
		if constructor, ok := vmap["constructor"]; ok {
			delete(keys, "constructor")
			res.Constructor = constructor.(string)
		}
		if module, ok := vmap["module"]; ok {
			delete(keys, "module")
			res.Module = module.(string)
		}
		if plugin, ok := vmap["plugin"]; ok {
			delete(keys, "plugin")
			res.Plugin = plugin.(string)
		}
		if params, ok := vmap["params"]; ok {
			delete(keys, "params")
			res.Params = params.(map[string]types.Value)
		}
		if len(keys) > 0 {
			err = errUnknownKeys("CfgBlockComponent", kv, keys)
		} else {
			resKV = &types.KeyValue{Key: kv.Key, Value: res}
		}
	} else {
		err = errUnknownValType("CfgBlockComponent", kv)
	}
	if err != nil {
		return nil, err
	}
	return resKV, nil
}

//============================================================================//

// MapCfgBlockPipelineMapper represents a mapper for pipeline config section.
type MapCfgBlockPipelineMapper struct{}

var _ Mapper = (*MapCfgBlockComponentMapper)(nil)

// Map converts map[string]Value to map[string]types.CfgBlockPipeline.
// All keys from the original map will be kept in the resulting map.
func (*MapCfgBlockPipelineMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := make(map[string]types.CfgBlockPipeline)
		for k, v := range vmap {
			res[k] = v.(types.CfgBlockPipeline)
		}
		return &types.KeyValue{Key: kv.Key, Value: res}, nil
	}
	return nil, errUnknownValType("map[string]CfgBlockPipeline", kv)
}

//============================================================================//

// CfgBlockPipelineMapper represents a mapper for pipeline.* config section.
type CfgBlockPipelineMapper struct{}

var _ Mapper = (*CfgBlockPipelineMapper)(nil)

// Map converts map[string]Value to types.CfgBlockPipeline structure.
// Lookup keys are:
// * connect
// * links
// * routes
// No extra keys are allowed in this section.
func (*CfgBlockPipelineMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	var resKV *types.KeyValue
	var err error
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := types.CfgBlockPipeline{}
		keys := make(map[string]struct{})
		for k := range vmap {
			keys[k] = struct{}{}
		}
		if connect, ok := vmap["connect"]; ok {
			delete(keys, "connect")
			res.Connect = connect.(string)
		}
		if links, ok := vmap["links"]; ok {
			delete(keys, "links")
			res.Links = links.([]string)
		}
		if routes, ok := vmap["routes"]; ok {
			delete(keys, "routes")
			res.Routes = routes.(map[string]string)
		}
		if len(keys) > 0 {
			err = errUnknownKeys("CfgBlockPipeline", kv, keys)
		} else {
			resKV = &types.KeyValue{Key: kv.Key, Value: res}
		}
	} else {
		err = errUnknownValType("CfgBlockPipeline", kv)
	}
	if err != nil {
		return nil, err
	}
	return resKV, nil
}

//============================================================================//

// ArrStrMapper represents a mapper for string arrays.
type ArrStrMapper struct{}

var _ Mapper = (*ArrStrMapper)(nil)

// Map converts []interface{} to []string.
func (*ArrStrMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	// []interface{}, not []Value because factual arguments are not being
	// type casted
	if arr, ok := kv.Value.([]interface{}); ok {
		res := make([]string, 0, len(arr))
		for _, v := range arr {
			res = append(res, v.(string))
		}
		return &types.KeyValue{Key: kv.Key, Value: res}, nil
	}
	return nil, errUnknownValType("[]string", kv)
}

//============================================================================//

// MapStrToStrMapper represents a mapper for map[string]string maps.
type MapStrToStrMapper struct{}

var _ Mapper = (*MapStrToStrMapper)(nil)

// Map converts from map[string]types.Value to map[string]string.
func (*MapStrToStrMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if mp, ok := kv.Value.(map[string]types.Value); ok {
		res := make(map[string]string)
		for k, v := range mp {
			res[k] = v.(string)
		}
		return &types.KeyValue{Key: kv.Key, Value: res}, nil
	}
	return nil, errUnknownValType("map[string]string", kv)
}
