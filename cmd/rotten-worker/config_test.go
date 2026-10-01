package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestStateSettingsDefaults(t *testing.T) {
	dir, age := stateSettings(&Configuration{ObservationInterval: 300})
	if dir != DefaultStateDir || age != 15*time.Minute {
		t.Errorf("defaults = %q, %v; want %q, 15m (three windows)", dir, age, DefaultStateDir)
	}
	dir, age = stateSettings(&Configuration{ObservationInterval: 300, StateDir: "/tmp/x", MaxSnapshotAge: 60})
	if dir != "/tmp/x" || age != time.Minute {
		t.Errorf("set = %q, %v; want /tmp/x, 1m", dir, age)
	}
}

// TestSampleConfDecodes: the sample conf in the repo root decodes, and sets
// the new state keys.
func TestSampleConfDecodes(t *testing.T) {
	b, err := os.ReadFile("../../conf")
	if err != nil {
		t.Fatal(err)
	}
	var c Configuration
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if c.StateDir != DefaultStateDir || c.MaxSnapshotAge != 3*c.ObservationInterval {
		t.Errorf("conf StateDir %q, MaxSnapshotAge %d; want %q and three windows", c.StateDir, c.MaxSnapshotAge, DefaultStateDir)
	}
}
