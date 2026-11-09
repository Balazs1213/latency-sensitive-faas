package core

// Node is the name of a cluster node.
type Node string

// NodeMetrics holds the CPU utilization and the memory usage reported for a node.
type NodeMetrics struct {
	Node   Node
	Cpu    cpu    `json:"cpu"`
	Memory memory `json:"memory"`
}

type cpu struct {
	Utilization float64 `json:"utilization"`
}

type memory struct {
	Usage float64 `json:"usage"`
}
