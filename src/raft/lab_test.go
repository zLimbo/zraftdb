package raft

import (
	"testing"
	"time"

	"6.824/zlog"
)

func TestBasicAgreeZ(t *testing.T) {
	servers := 7
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test Z: basic agreement")

	t0 := time.Now()
	var leader *Raft

OutLoop:
	for time.Since(t0).Seconds() < 5 {
		for i := range cfg.rafts {
			if _, isLeader := cfg.rafts[i].GetState(); isLeader {
				leader = cfg.rafts[i]
				break OutLoop
			}
		}
	}

	zlog.Info("leader: %d", leader.me)

	t1 := time.Now()

	reqCount := int64(0)
	for time.Since(t0).Seconds() < 5 {
		leader.Start(reqCount)
		reqCount++
	}

	cfg.end()

	if cfg.t.Failed() == false {

	}
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	ncmds := cfg.maxIndex - cfg.maxIndex0 // number of Raft agreements reported

	take := time.Since(t1).Seconds()
	tps := float64(ncmds) / take
	traffic := float64(ncmds) * 8 / take / 1024 / 1024

	time.Sleep(100 * time.Millisecond)
	zlog.Info("reqCount=%d, ncmds=%d, take=%.2fs, tps=%.2f, traffic=%.2fMB", reqCount, ncmds, take, tps, traffic)
}
