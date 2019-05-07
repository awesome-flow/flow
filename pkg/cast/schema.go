package cast

// Schema is a pretty flexible structure for schema definitions.
// It might be:
// * a Mapper
// * a Converter
// * a map[string]Schema
type Schema interface{}

var (
	// ConfigSchema defines the global flow configuration schema.
	// The settings defined here are system-wide and system modules reply
	// upon that.
	ConfigSchema Schema
)

func init() {
	ConfigSchema = Schema(map[string]Schema{
		"__self__": nil,
		"system": map[string]Schema{
			"__self__": &CfgBlockSystemMapper{},
			"maxprocs": ToInt,
			"admin": map[string]Schema{
				"__self__":  &CfgBlockSystemAdminMapper{},
				"enabled":   ToBool,
				"bind_addr": ToStr,
			},
			"metrics": map[string]Schema{
				"__self__": &CfgBlockSystemMetricsMapper{},
				"enabled":  ToBool,
				"interval": ToInt,
				"receiver": map[string]Schema{
					"__self__": &CfgBlockSystemMetricsReceiverMapper{},
					"type":     ToStr,
					"params": map[string]Schema{
						"__self__": nil,
						"*":        Identity,
					},
				},
			},
		},
		"components": map[string]Schema{
			"__self__": &MapCfgBlockComponentMapper{},
			"*": map[string]Schema{
				"__self__":    &CfgBlockComponentMapper{},
				"constructor": ToStr,
				"module":      ToStr,
				"plugin":      ToStr,
				"params": map[string]Schema{
					"__self__": nil,
					"*":        Identity,
				},
			},
		},
		"pipeline": map[string]Schema{
			"__self__": &MapCfgBlockPipelineMapper{},
			"*": map[string]Schema{
				"__self__": &CfgBlockPipelineMapper{},
				"connect":  ToStr,
				"links":    &ArrStrMapper{},
				"routes": map[string]Schema{
					"__self__": &MapStrToStrMapper{},
					"*":        ToStr,
				},
			},
		},

		/* non-serialisable attributes */

		"config": map[string]Schema{
			"__self__": nil,
			"path":     ToStr,
		},
		"plugin": map[string]Schema{
			"__self__": nil,
			"path":     ToStr,
		},
	})
}
