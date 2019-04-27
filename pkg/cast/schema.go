package cast

type Schema interface{}

var (
	ConfigSchema Schema
)

func init() {
	ConfigSchema = Schema(map[string]Schema{
		"__self__": &CfgMapper{},
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
			"__self__": nil, //TODO
			"*": map[string]Schema{
				"__self__": nil, //TODO
				"connect":  ToStr,
				"links":    Identity, //TODO
				"routes":   Identity, //TODO
			},
		},
	})
}
