package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type Command struct {
	Seq    int64
	FromId int64
	Tx     []byte
}

type Log struct {
	cmd *Command
}

type Stage = int32

const (
	PrepareStage = iota
	CommitStage
	ReplyStage
)

type CmdCert struct {
	seq        int64
	digest     []byte
	start      time.Time
	replyCount int
	replys     map[int64][]byte
}

type LogCert struct {
	cmd      *Command
	digest   []byte
	prepares map[int64][]byte
	commits  map[int64][]byte
	prepareQ []*PrepareArgs
	commitQ  []*CommitArgs
	stage    Stage
	mu       sync.Mutex
}

func (lc *LogCert) setCmd(cmd *Command) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.cmd = cmd
}

func (lc *LogCert) getCmd() *Command {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.cmd
}

func (lc *LogCert) prepareVote(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareQ = append(lc.prepareQ, args)
}

func (lc *LogCert) commitVote(args *CommitArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commitQ = append(lc.commitQ, args)
}

func (lc *LogCert) prepareBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, msg := range lc.prepareQ {
		// todo 检查
		lc.prepares[msg.NodeId] = msg.Sign
	}
	lc.prepareQ = make([]*PrepareArgs, 0)
	return len(lc.prepares)
}

func (lc *LogCert) commitBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, msg := range lc.commitQ {
		// todo 检查
		lc.commits[msg.NodeId] = msg.Sign
	}
	lc.commitQ = make([]*CommitArgs, 0)
	return len(lc.commits)
}

func (lc *LogCert) getStage() Stage {
	return atomic.LoadInt32(&lc.stage)
}

func (lc *LogCert) setStage(stage Stage) {
	atomic.StoreInt32(&lc.stage, stage)
}
