package types

// Cfg is the global config structure, aggregates all other kinds of
// config blocks.
type Cfg struct {
	Actors   map[string]CfgBlockActor
	Pipeline map[string]CfgBlockPipeline
	System   CfgBlockSystem
}

// CfgBlockSystem represents the system part of the config: the block
// representing settings for admin interface, metrics collection, threadiness
// etc.
type CfgBlockSystem struct {
	Admin    CfgBlockSystemAdmin
	Maxprocs int
	Metrics  CfgBlockSystemMetrics
}

// CfgBlockSystemAdmin represents settings for admin interface.
type CfgBlockSystemAdmin struct {
	Bind    string
	Enabled bool
}

// CfgBlockSystemMetrics represents system metrics module settings: sending
// interval and the receiver.
type CfgBlockSystemMetrics struct {
	Enabled  bool
	Interval int
	Receiver CfgBlockSystemMetricsReceiver
}

// CfgBlockSystemMetricsReceiver represents settings for system metrics
// receiver: it's type and parameters.
type CfgBlockSystemMetricsReceiver struct {
	Params map[string]Value
	Type   string
}

// CfgBlockActor represents a singular component config: it's module name,
// parameter list and (if applicable) plugin name and the corresponding
// constructor.
type CfgBlockActor struct {
	Constructor string
	Module      string
	Params      map[string]Value
	Plugin      string
}

// CfgBlockPipeline represents a singular pipeline config: one of 3: connection,
// a link set or a routing map.
type CfgBlockPipeline struct {
	Connect []string
}

// IsDisconnected is a helper method indicating a pipeline block has no
// outgoing connections: 0 connections, 0 links and no routes.
func (cbp CfgBlockPipeline) IsDisconnected() bool {
	return len(cbp.Connect) == 0
}
