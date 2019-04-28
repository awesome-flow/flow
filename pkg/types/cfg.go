package types

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

func (cbp CfgBlockPipeline) IsDisconnected() bool {
	return len(cbp.Links) == 0 &&
		len(cbp.Connect) == 0 &&
		len(cbp.Routes) == 0
}
