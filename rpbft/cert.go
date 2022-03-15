package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type Log struct {
	op []byte
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
	replys     map[int64][]byte
}

type LogCert struct {
	seq      int64
	view int64
	req      *RequestArgs
	digest   []byte
	prepares map[int64]*PrepareArgs
	commits  map[int64]*CommitArgs
	prepareQ []*PrepareArgs
	commitQ  []*CommitArgs
	stage    Stage
	mu       sync.Mutex
}

func (lc *LogCert) set(req *RequestArgs, digest []byte, view int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.req = req
	lc.digest = digest
}

func (lc *LogCert) get() (*RequestArgs, []byte, int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.req, lc.digest, lc.view
}

func (lc *LogCert) pushPrepare(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareQ = append(lc.prepareQ, args)
}

func (lc *LogCert) pushCommit(args *CommitArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commitQ = append(lc.commitQ, args)
}

func (lc *LogCert) popAllPrepares() []*PrepareArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.prepareQ
	lc.prepareQ = nil
	return argsQ
}

func (lc *LogCert) popAllCommits() []*CommitArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.commitQ
	lc.commitQ = nil
	return argsQ
}

func (lc *LogCert) getStage() Stage {
	return atomic.LoadInt32(&lc.stage)
}

func (lc *LogCert) setStage(stage Stage) {
	atomic.StoreInt32(&lc.stage, stage)
}

func (lc *LogCert) prepareVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.prepares[nodeId]
	return ok
}

func (lc *LogCert) commitVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.commits[nodeId]
	return ok
}

func (lc *LogCert) prepareVote(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepares[args.Msg.NodeId] = args
}

func (lc *LogCert) commitVote(args *CommitArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commits[args.Msg.NodeId] = args
}

func (lc *LogCert) prepareBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.prepares)
}

func (lc *LogCert) commitBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.commits)
}

type RequestMsg struct {
	Operator []byte
	Timestamp int64
	ClientId int64
}

type PrePrepareMsg struct {
	View int64
	Seq int64
	Digest []byte
	NodeId int64
}

type PrepareMsg struct {
	View int64
	Seq int64
	Digest []byte
	NodeId int64
}

type CommitMsg struct {
	View int64
	Seq int64
	Digest []byte
	NodeId int64
}

type ReplyMsg struct {
	View int64
	Seq int64
	Timestamp int64
	ClientId int64
	NodeId int64
	Result []byte
}

type RequestArgs struct {
	Req *RequestMsg
	Sign   []byte
}

type RequestReply struct {
	Seq int64
	Ok bool
}

type PrePrepareArgs struct {
	Msg *PrePrepareMsg
	Sign   []byte
	ReqArgs *RequestArgs
}

type PrepareArgs struct {
	Msg *PrepareMsg
	Sign   []byte
}

type CommitArgs struct {
	Msg *CommitMsg
	Sign   []byte
}

type ReplyArgs struct {
	Msg *ReplyMsg
	Sign   []byte
}