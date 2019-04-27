package cast

type Schema interface{}

var (
	ConfigSchema Schema
)

func init() {
	ConfigSchema = Schema(map[string]Schema{
		"system": map[string]Schema{
			"__self__": nil, //TODO
			"maxprocs": ToInt,
			"admin": map[string]Schema{
				"__self__":  nil, //TODO
				"enabled":   ToBool,
				"bind_addr": ToStr,
			},
			"metrics": map[string]Schema{
				"__self__": nil, //TODO
				"enabled":  ToBool,
				"interval": ToInt,
				"receiver": map[string]Schema{
					"__self__": nil, //TODO
					"type":     ToStr,
					"params": map[string]Schema{
						"__self__": nil, //TODO
						"*":        ToStr,
					},
				},
			},
		},
		"components": map[string]Schema{
			"__self__": nil, //TODO
			"*": map[string]Schema{
				"__self__":    nil, //TODO
				"constructor": ToStr,
				"module":      ToStr,
				"plugin":      ToStr,
				"params":      Identity, //TODO
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
