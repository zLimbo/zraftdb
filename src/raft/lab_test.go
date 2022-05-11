package raft

import (
	"fmt"
	"testing"
	"time"

	"6.824/zlog"
)

func TestBasicAgreeZ(t *testing.T) {
	reqTime := 5.0 // 请求时间
	reqSize := 128
	batchSize := 1
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

	format := fmt.Sprintf("%%0%dd", reqSize*batchSize)

	for time.Since(t0).Seconds() < reqTime {
		leader.Start(fmt.Sprintf(format, reqCount))
		reqCount++
	}

	cfg.end()

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	ncmds := cfg.maxIndex - cfg.maxIndex0 // number of Raft agreements reported

	take := time.Since(t1).Seconds()
	tps := float64(ncmds) / take
	traffic := float64(ncmds) * float64(reqSize) * float64(batchSize) / take / 1024 / 1024

	time.Sleep(100 * time.Millisecond)
	zlog.Info("reqCount=%d, ncmds=%d, take=%.2fs, tps=%.2f, traffic=%.2fMB", reqCount, ncmds, take, tps, traffic)

	s := fmt.Sprintf(format, reqCount)
	zlog.Info("[%s], len=%d", s, len(s))
}
