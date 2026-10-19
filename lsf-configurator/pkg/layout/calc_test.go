package layout

import (
	"testing"

	"lsf-configurator/pkg/core"
)

// A group holding every component must still see the external input rate.
func TestArrivalRateIncludesEntryRate(t *testing.T) {
	group := []core.ComponentProfile{{Name: "a"}, {Name: "b"}}
	links := []core.ScenarioLink{{From: "a", To: "b", InvocationRate: 50}}
	if got := calculateTotalArrivalRate(group, links, "a", 50); got != 50 {
		t.Fatalf("expected 50, got %v", got)
	}
}

// A group without the entry component gets only its incoming links (unchanged behaviour).
func TestArrivalRateWithoutEntry(t *testing.T) {
	group := []core.ComponentProfile{{Name: "b"}}
	links := []core.ScenarioLink{{From: "a", To: "b", InvocationRate: 50}}
	if got := calculateTotalArrivalRate(group, links, "a", 50); got != 50 {
		t.Fatalf("expected 50, got %v", got)
	}
}

// Before the fix this case returned the 1.0 fallback.
func TestArrivalRateOnlyEntry(t *testing.T) {
	group := []core.ComponentProfile{{Name: "a"}}
	if got := calculateTotalArrivalRate(group, nil, "a", 30); got != 30 {
		t.Fatalf("expected 30, got %v", got)
	}
}