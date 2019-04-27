package cast

type Cfg struct {
	Components map[string]CfgBlockComponent
	Pipeline   map[string]CfgBlockPipeline
	System     *CfgBlockSystem
}

type CfgBlockSystem struct {
	Admin    CfgBlockSystemAdmin
	Maxprocs int
	Metrics  CfgBlockSystemMetrics
}

type CfgBlockSystemAdmin struct {
	BindAddr string `yaml:"bind_addr"`
	Enabled  bool
}

type CfgBlockSystemMetrics struct {
	Enabled  bool
	Interval int
	Receiver CfgBlockSystemMetricsReceiver
}

type CfgBlockSystemMetricsReceiver struct {
	Params map[string]string
	Type   string
}

type CfgBlockComponent struct {
	Constructor string
	Module      string
	Params      map[string]interface{}
	Plugin      string
}

type CfgBlockPipeline struct {
	Connect string
	Links   []string
	Routes  map[string]string
}
