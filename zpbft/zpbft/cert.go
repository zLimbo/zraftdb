package zpbft

import (
	"sync"
	"sync/atomic"
)

// pbft 三阶段状态
type Stage = int32

const (
	RequestStage = iota
	PrePrepareStage
	PrepareStage
	CommitStage
	ReplyStage
)

// 某次共识的消息存证
type LogCert struct {
	mu         sync.Mutex
	seq        int64
	view       int32
	stage      Stage
	req        *RequestArgs
	digest     []byte
	id2prepare map[int32]*PrepareArgs
	id2commit  map[int32]*CommitArgs
	prepareWQ  []*PrepareArgs
	commitWQ   []*CommitArgs
}

func (lc *LogCert) set(req *RequestArgs, digest []byte, view int32) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.req = req
	lc.digest = digest
}

func (lc *LogCert) get() (*RequestArgs, []byte, int32) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.req, lc.digest, lc.view
}

func (lc *LogCert) pushPrepare(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareWQ = append(lc.prepareWQ, args)
}

func (lc *LogCert) pushCommit(args *CommitArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commitWQ = append(lc.commitWQ, args)
}

func (lc *LogCert) popAllPrepares() []*PrepareArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.prepareWQ
	lc.prepareWQ = nil
	return argsQ
}

func (lc *LogCert) popAllCommits() []*CommitArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.commitWQ
	lc.commitWQ = nil
	return argsQ
}

func (lc *LogCert) getStage() Stage {
	return atomic.LoadInt32(&lc.stage)
}

func (lc *LogCert) setStage(stage Stage) {
	atomic.StoreInt32(&lc.stage, stage)
}

func (lc *LogCert) prepareVoted(PeerId int32) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.id2prepare[PeerId]
	return ok
}

func (lc *LogCert) commitVoted(PeerId int32) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.id2commit[PeerId]
	return ok
}

func (lc *LogCert) prepareVote(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.id2prepare[args.Msg.PeerId] = args
}

func (lc *LogCert) commitVote(args *CommitArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.id2commit[args.Msg.PeerId] = args
}

func (lc *LogCert) prepareBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.id2prepare)
}

func (lc *LogCert) commitBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.id2commit)
}

type RequestMsg struct {
	ClientAddr string
	Timestamp  int64
	Command    string
}

type PrePrepareMsg struct {
	PeerId int32
	View   int32
	Seq    int64
	Digest []byte
}

type PrepareMsg struct {
	PeerId int32
	View   int32
	Seq    int64
	Digest []byte
}

type CommitMsg struct {
	PeerId int32
	View   int32
	Seq    int64
	Digest []byte
}

type ReplyMsg struct {
	PeerId     int32
	View       int32
	Seq        int64
	Timestamp  int64
	ClientAddr string
	Result     string
}

type RequestArgs struct {
	Req  *RequestMsg
	Sign []byte
}

type RequestReply struct {
	Seq int64
	Ok  bool
}

type PrePrepareArgs struct {
	Msg     *PrePrepareMsg
	Sign    []byte
	ReqArgs *RequestArgs
}

type PrePrepareReply struct {
	Ok bool
}

type PrepareArgs struct {
	Msg  *PrepareMsg
	Sign []byte
}

type PrepareReply struct {
	Ok bool
}

type CommitArgs struct {
	Msg  *CommitMsg
	Sign []byte
}

type CommitReply struct {
	Ok bool
}

type ReplyArgs struct {
	Msg  *ReplyMsg
	Sign []byte
}

type ReplyReply struct {
	Ok bool
}
