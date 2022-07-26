package zraft

import "zraft/zlog"

type Config struct {
	EpochSize  int
	EpochNum   int
	ReqSize    int
	BatchSize  int
	Persisted  bool
	LogDir     string
	DelayFrom  int
	DelayRange int
	Draft      bool
}

var KConf Config

func (c *Config) Show() {
	zlog.Info("> EpochSize=%d", c.EpochSize)
	zlog.Info("> EpochNum=%d", c.EpochNum)
	zlog.Info("> ReqSize=%d", c.ReqSize)
	zlog.Info("> BatchSize=%d", c.BatchSize)
	zlog.Info("> Persisted=%v", c.Persisted)
	zlog.Info("> LogDir=%s", c.LogDir)
	zlog.Info("> DelayFrom=%d", c.DelayFrom)
	zlog.Info("> DelayRange=%d", c.DelayRange)
	zlog.Info("> Draft=%v", c.Draft)
}
