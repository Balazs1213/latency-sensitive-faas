package core

// Spare pod modes: pods on top of the pods a composition of the active layout needs, so that losing a pod does
// not stop the composition
const (
	// SparePodsOff runs no spare pods
	SparePodsOff = "off"
	// SparePodsSingle gives spare pods only to the compositions planned for a single pod: losing that pod stops the
	// composition, while a composition planned for several pods only loses part of its capacity
	SparePodsSingle = "single"
	// SparePodsAll gives spare pods to every composition
	SparePodsAll = "all"
)

// sparePods returns how many spare pods a composition of the active layout runs on top of the pods it needs
func sparePods(mode string, count int, ci CompositionInfo) int {
	switch {
	case mode == SparePodsAll, mode == SparePodsSingle && ci.RequiredReplicas <= 1:
		return count
	default:
		return 0
	}
}

// spareFor returns the spare pods of a composition of the active layout in the configured spare pod mode
func (c *latencyController) spareFor(ci CompositionInfo) int {
	return sparePods(c.sparePodMode, c.sparePodCount, ci)
}
