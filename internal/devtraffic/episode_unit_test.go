package devtraffic_test

import (
	"context"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/devtraffic"
)

func TestScheduledEpisode(t *testing.T) {
	const every, length = 15 * time.Minute, 2 * time.Minute
	base := time.Unix(0, 0).UTC().Add(1000 * every)
	seen := map[devtraffic.Episode]bool{}
	for slot := range 3 {
		start := base.Add(time.Duration(slot) * every)
		for _, off := range []time.Duration{0, time.Second, length - time.Second} {
			ep, at := devtraffic.ScheduledEpisode(every, length, start.Add(off))
			if ep == devtraffic.NoEpisode || !at.Equal(start) {
				t.Errorf("slot %d +%v: %q starting %v, want an episode starting %v", slot, off, ep, at, start)
			}
			if slot > 0 && seen[ep] && off == 0 {
				t.Errorf("slot %d repeats %q before the others ran", slot, ep)
			}
			seen[ep] = true
		}
		for _, off := range []time.Duration{length, every / 2, every - time.Second} {
			if ep, _ := devtraffic.ScheduledEpisode(every, length, start.Add(off)); ep != devtraffic.NoEpisode {
				t.Errorf("slot %d +%v: %q, want none after the episode", slot, off, ep)
			}
		}
	}
	for _, ep := range devtraffic.Episodes() {
		if !seen[ep] {
			t.Errorf("three slots in a row never ran %q", ep)
		}
	}
	if ep, _ := devtraffic.ScheduledEpisode(0, length, base); ep != devtraffic.NoEpisode {
		t.Errorf("every 0 gave %q, want episodes off", ep)
	}
}

func TestEpisodesGateSlowness(t *testing.T) {
	sz := devtraffic.SizesFor(1)
	r := rand.New(rand.NewPCG(1, 2))
	c := devtraffic.Contexts()[0]
	m := devtraffic.Meta{ContextID: "x", Hostname: "h", PID: 1}
	render := func(name string, ep devtraffic.Episode) []any {
		s, ok := devtraffic.ShapeByName(name)
		if !ok {
			t.Fatalf("no shape %s", name)
		}
		_, args := s.RenderIn(c, m, "lms_shard_1", devtraffic.Trailing, r, sz, ep)
		return args
	}
	others := []devtraffic.Episode{devtraffic.NoEpisode, devtraffic.SlowRead, devtraffic.LockWait}
	for range 500 {
		for _, ep := range others {
			if d := render("course_activity", ep)[1].(float64); d != 0 {
				t.Fatalf("course_activity sleeps %v outside a sleep episode (%q)", d, ep)
			}
		}
		if d := render("course_activity", devtraffic.SlowSleep)[1].(float64); d < 0.3 || d > 0.6 {
			t.Fatalf("course_activity sleeps %v in a sleep episode, want 0.3 to 0.6", d)
		}
		if id := render("touch_user", devtraffic.LockWait)[0].(int64); id < 1 || id > devtraffic.HotUsers {
			t.Fatalf("touch_user picks user %d in a lock episode, want one of the %d hot users", id, devtraffic.HotUsers)
		}
	}
	cold := 0
	for range 200 {
		if render("touch_user", devtraffic.NoEpisode)[0].(int64) > devtraffic.HotUsers {
			cold++
		}
	}
	if cold < 150 {
		t.Errorf("only %d of 200 touch_user calls outside episodes miss the hot users", cold)
	}
	holder, ok := devtraffic.ShapeByName(devtraffic.LockShape)
	if !ok {
		t.Fatalf("no lock holder shape %s", devtraffic.LockShape)
	}
	sql, args := holder.RenderIn(c, m, "lms_shard_1", devtraffic.Trailing, r, sz, devtraffic.LockWait)
	if !strings.Contains(sql, "UPDATE lms_shard_1.users") || args[0] != int64(devtraffic.HotUsers) {
		t.Errorf("lock holder %q %v doesn't lock the hot users", sql, args)
	}
}

func TestRunRejectsBadEpisodes(t *testing.T) {
	for _, cfg := range []devtraffic.Config{
		{EpisodeEvery: time.Minute, EpisodeLength: time.Minute},
		{EpisodeEvery: time.Minute, EpisodeLength: -time.Second},
		{EpisodeEvery: -time.Minute},
		{Episode: "meltdown"},
	} {
		if _, err := devtraffic.Run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "episode") {
			t.Errorf("Run(%+v) = %v, want an episode error", cfg, err)
		}
	}
}
