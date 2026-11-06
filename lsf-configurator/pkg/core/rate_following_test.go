package core

import (
	"math"
	"testing"
	"time"
)

// newTestController returns a controller with the capacity model settings of the measurements:
// follow mode, target utilization 0.6 and switching at the full layout capacity
func newTestController() *latencyController {
	return &latencyController{
		prewarmMode:       "follow",
		targetUtilization: 0.6,
		switchRateFactor:  1.0,
		metricWindow:      15 * time.Second,
		componentRuntimes: make(map[string]map[string]ComponentRuntime),
		rateHistory:       make(map[string][]ratePoint),
		downgradeSince:    make(map[string]time.Time),
	}
}

// The compositions of the ADAS app with its profiled runtimes (ms) and planned replicas
func fusedComposition() CompositionInfo {
	return CompositionInfo{
		ComponentProfiles: []ComponentProfile{
			{Name: "detect-track-service", Runtime: 357},
			{Name: "position-service", Runtime: 2},
			{Name: "ru-fusion-service", Runtime: 3},
		},
		RequiredReplicas:  2,
		TargetConcurrency: 1,
	}
}

func detectComposition() CompositionInfo {
	return CompositionInfo{
		ComponentProfiles: []ComponentProfile{{Name: "detect-track-service", Runtime: 357}},
		RequiredReplicas:  9,
		TargetConcurrency: 1,
	}
}

func positionFusionComposition() CompositionInfo {
	return CompositionInfo{
		ComponentProfiles: []ComponentProfile{
			{Name: "position-service", Runtime: 2},
			{Name: "ru-fusion-service", Runtime: 3},
		},
		RequiredReplicas:  1,
		TargetConcurrency: 1,
	}
}

// testApp is the ADAS app with its two layouts; the fused (min) layout is active
func testApp() *FunctionApp {
	return &FunctionApp{
		Id: "app",
		LayoutCandidates: map[string]Layout{
			LayoutKeyMin: {"knative": fusedComposition()},
			LayoutKeyMax: {"knative": detectComposition(), "knative-m02": positionFusionComposition()},
		},
		ActiveLayoutKey: LayoutKeyMin,
	}
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

// One pod serves (1000 / t) * C * U requests per second
func TestPodCapacity(t *testing.T) {
	c := newTestController()
	want := 1000.0 / 362 * 0.6
	if got := c.podCapacity("app", fusedComposition()); !almostEqual(got, want) {
		t.Fatalf("pod capacity: want %v, got %v", want, got)
	}
}

// Without a runtime profile the capacity is unknown
func TestPodCapacityWithoutRuntime(t *testing.T) {
	c := newTestController()
	ci := CompositionInfo{ComponentProfiles: []ComponentProfile{{Name: "x"}}, RequiredReplicas: 3}
	if got := c.podCapacity("app", ci); got != 0 {
		t.Fatalf("no runtime profile: want 0, got %v", got)
	}
	if got := c.podsForRate("app", ci, 5); got != 3 {
		t.Fatalf("no runtime profile: want the planned 3 replicas, got %d", got)
	}
}

// The replica formula with the measured rate, between one pod and the planned replicas
func TestPodsForRate(t *testing.T) {
	c := newTestController()
	cases := []struct {
		rate float64
		want int
	}{
		{0, 1},   // at least one pod
		{1.5, 1}, // below one pod's capacity (1.66 req/s)
		{1.7, 2}, // above it
		{10, 2},  // at most the planned replicas
	}
	for _, tc := range cases {
		if got := c.podsForRate("app", fusedComposition(), tc.rate); got != tc.want {
			t.Errorf("rate %v: want %d pods, got %d", tc.rate, tc.want, got)
		}
	}
}

// A layout is sized for the rate of its weakest composition
func TestLayoutCapacity(t *testing.T) {
	c := newTestController()
	app := testApp()
	if got, want := c.layoutCapacity("app", app.LayoutCandidates[LayoutKeyMin]), 2*1000.0/362*0.6; !almostEqual(got, want) {
		t.Errorf("fused layout: want %v, got %v", want, got)
	}
	// The split layout is limited by the detect (9 pods), not by position+fusion (1 fast pod)
	if got, want := c.layoutCapacity("app", app.LayoutCandidates[LayoutKeyMax]), 9*1000.0/357*0.6; !almostEqual(got, want) {
		t.Errorf("split layout: want %v, got %v", want, got)
	}
}

// The proactive switch fires above the active layout's capacity (3.31 req/s for the fused layout)
func TestRateAboveCapacity(t *testing.T) {
	c := newTestController()
	app := testApp()
	if c.rateAboveCapacity(app, LayoutKeyMin, 3.0) {
		t.Error("3.0 req/s fits the fused layout")
	}
	if !c.rateAboveCapacity(app, LayoutKeyMin, 3.5) {
		t.Error("3.5 req/s is above the fused layout's capacity")
	}
	c.switchRateFactor = 0
	if c.rateAboveCapacity(app, LayoutKeyMin, 100) {
		t.Error("a switch factor of 0 turns rate-based switching off")
	}
}

// A growing rate is extrapolated by its growth to half a window plus one pod startup ahead
func TestForecastRateGrowing(t *testing.T) {
	c := newTestController()
	c.rateForecast = true
	c.startupMean = 20 * time.Second
	c.startupSamples = 1
	t0 := time.Unix(1000, 0)
	if got := c.forecastRate("app", 2, t0); got != 2 {
		t.Fatalf("first rate, no growth known yet: want 2, got %v", got)
	}
	got := c.forecastRate("app", 3, t0.Add(10*time.Second))
	want := 3 + 0.1*(7.5+20) // growth 0.1 req/s per second
	if !almostEqual(got, want) {
		t.Fatalf("growing rate: want %v, got %v", want, got)
	}
}

// A falling rate is not extrapolated: scaling down is left to the hold time
func TestForecastRateFalling(t *testing.T) {
	c := newTestController()
	c.rateForecast = true
	t0 := time.Unix(1000, 0)
	c.forecastRate("app", 3, t0)
	if got := c.forecastRate("app", 2, t0.Add(10*time.Second)); got != 2 {
		t.Fatalf("falling rate: want 2, got %v", got)
	}
}

// Without the forecast the pods are sized for the measured rate
func TestForecastRateOff(t *testing.T) {
	c := newTestController()
	t0 := time.Unix(1000, 0)
	c.forecastRate("app", 2, t0)
	if got := c.forecastRate("app", 3, t0.Add(10*time.Second)); got != 3 {
		t.Fatalf("forecast off: want 3, got %v", got)
	}
}

// The scale-down hold is the measured pod startup time (ski rental), 60 s until it is measured
func TestScaleDownHold(t *testing.T) {
	c := newTestController()
	if got := c.scaleDownHold(); got != rateScaleDownHold {
		t.Errorf("not measured yet: want %v, got %v", rateScaleDownHold, got)
	}
	c.startupMean = 18 * time.Second
	c.startupSamples = 3
	if got := c.scaleDownHold(); got != 18*time.Second {
		t.Errorf("measured: want 18s, got %v", got)
	}
}

// Follow mode switches back once the rate has fitted the smaller layout for the hold time
func TestRateDowngradeDue(t *testing.T) {
	c := newTestController()
	c.startupMean = time.Nanosecond // hold time: practically none
	c.startupSamples = 1
	app := testApp()
	app.ActiveLayoutKey = LayoutKeyMax

	if c.rateDowngradeDue(app, 5) {
		t.Fatal("5 req/s does not fit the fused layout: no switch back")
	}
	if c.rateDowngradeDue(app, 1) {
		t.Fatal("the rate fits now: the hold starts, no switch back yet")
	}
	if !c.rateDowngradeDue(app, 1) {
		t.Fatal("the rate fitted for the hold time: switch back")
	}
	if _, waiting := c.downgradeSince[app.Id]; waiting {
		t.Fatal("after the switch back the hold must restart")
	}

	app.ActiveLayoutKey = LayoutKeyMin
	if c.rateDowngradeDue(app, 0) {
		t.Fatal("no smaller layout than the fused one")
	}
}
