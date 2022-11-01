package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type txPoolUnit struct {
	txNum int64
	arrivalTime time.Time
	completeTime time.Time
	completed bool
}

type Log struct {
	Op []byte
}

type Stage = int32

const (
	InitialStage = iota
	DuplicatedStage
	PrepareStage
	CommitStage
	ReplyStage
)

type CmdCert struct {
	seq    int64
	digest []byte
	start  time.Time
	replys map[int64][]byte
}

type LogCert struct {
	seq              int64
	duplicator          int64
	req              *RequestArgs
	digest           []byte
	prepareConfirmed bool
	committed        bool
	prepares         map[int64]*PrepareArgs  // 用作prepare计数
	commits  map[int64]*CommitArgs   // 用作commit计数
	prepareConfirms map[int64]*PrepareConfirmArgs  // 用作prepareConfirm计数
	duplicateConfirms map[int64]*DuplicateConfirmArgs  // 用作duplicateConfirm计数
	commitConfirms  map[int64]*CommitConfirmArgs   // 用作commitShare计数
	prepareQ []*PrepareArgs
	prepareConfirmQ []*PrepareConfirmArgs
	duplicateConfirmQ []*DuplicateConfirmArgs
	commitQ  []*CommitArgs
	commitConfirmQ  []*CommitConfirmArgs
	stage    Stage
	mu       sync.Mutex
	prepared       sync.Mutex
	committedMutex sync.Mutex
	//for PRaft
	term             int64
	logIndex         int64
	produceTime time.Time
	completeTime time.Time
}

func (lc *LogCert) set(req *RequestArgs, digest []byte, logIndex int64, duplicator int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.req = req
	lc.digest = digest
	lc.logIndex	= logIndex
	lc.duplicator = duplicator
}

func (lc *LogCert) logCommitted() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.req != nil{
		//lc.req.Req.Operator = nil
		//lc.req = nil
		lc.digest = nil
		lc.committed = true
		//lc.prepares = nil
		//lc.commits = nil
		//lc.prepareShares = nil
		//lc.commitShares = nil
		//lc.prepareQ = nil
		//lc.prepareShareQ = nil
		//lc.commitQ = nil
		//lc.commitShareQ = nil
	}
}

func (lc *LogCert) get() (*RequestArgs, []byte, int64, int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.req, lc.digest, lc.duplicator, lc.logIndex
}

func (lc *LogCert) pushPrepare(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareQ = append(lc.prepareQ, args)
}

func (lc *LogCert) pushPrepareConfirm(args *PrepareConfirmArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareConfirmQ = append(lc.prepareConfirmQ, args)
}

func (lc *LogCert) pushDuplicateConfirm(args *DuplicateConfirmArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.duplicateConfirmQ = append(lc.duplicateConfirmQ, args)
}

func (lc *LogCert) pushCommit(args *CommitArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commitQ = append(lc.commitQ, args)
}

func (lc *LogCert) pushCommitConfirm(args *CommitConfirmArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commitConfirmQ = append(lc.commitConfirmQ, args)
}

func (lc *LogCert) popAllPrepares() []*PrepareArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.prepareQ
	lc.prepareQ = nil
	return argsQ
}

func (lc *LogCert) popAllPrepareConfirms() []*PrepareConfirmArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.prepareConfirmQ
	lc.prepareConfirmQ = nil
	return argsQ
}

func (lc *LogCert) popAllDuplicateConfirms() []*DuplicateConfirmArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.duplicateConfirmQ
	lc.duplicateConfirmQ = nil
	return argsQ
}

//func (lc *LogCert) popAllCommits() []*CommitArgs {
//	lc.mu.Lock()
//	defer lc.mu.Unlock()
//	argsQ := lc.commitQ
//	lc.commitQ = nil
//	return argsQ
//}

//func (lc *LogCert) popAllCommitShares() []*CommitShareArgs {
//	lc.mu.Lock()
//	defer lc.mu.Unlock()
//	argsQ := lc.commitShareQ
//	lc.commitShareQ = nil
//	return argsQ
//}

func (lc *LogCert) getStage() Stage {
	return atomic.LoadInt32(&lc.stage)
}

func (lc *LogCert) setStage(stage Stage) {
	atomic.StoreInt32(&lc.stage, stage)
}

func (lc *LogCert) prepareConfirmVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.prepareConfirms[nodeId]
	return ok
}

func (lc *LogCert) duplicateConfirmVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.duplicateConfirms[nodeId]
	return ok
}

func (lc *LogCert) prepareVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.prepares[nodeId]
	return ok
}

//func (lc *LogCert) commitShareVoted(nodeId int64) bool {
//	lc.mu.Lock()
//	defer lc.mu.Unlock()
//	_, ok := lc.commitShares[nodeId]
//	return ok
//}

//func (lc *LogCert) commitVoted(nodeId int64) bool {
//	lc.mu.Lock()
//	defer lc.mu.Unlock()
//	_, ok := lc.commits[nodeId]
//	return ok
//}

func (lc *LogCert) prepareConfirmVote(args *PrepareConfirmArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareConfirms[args.Msg.NodeId] = args
}

func (lc *LogCert) duplicateConfirmVote(args *DuplicateConfirmArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.duplicateConfirms[args.Msg.NodeId] = args
}

func (lc *LogCert) prepareVote(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepares[args.Msg.NodeId] = args
}

//func (lc *LogCert) commitShareVote(args *CommitShareArgs) {
//	lc.mu.Lock()
//	defer lc.mu.Unlock()
//	lc.commitShares[args.Msg.NodeId] = args
//}

//func (lc *LogCert) commitVote(args *CommitArgs) {
//	lc.mu.Lock()
//	defer lc.mu.Unlock()
//	lc.commits[args.Msg.NodeId] = args
//}

func (lc *LogCert) prepareConfirmBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.prepareConfirms)
}

func (lc *LogCert) duplicateConfirmBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.duplicateConfirms)
}

func (lc *LogCert) prepareBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.prepares)
}

//func (lc *LogCert) commitShareBallot() int {
//	lc.mu.Lock()
//	defer lc.mu.Unlock()
//	return len(lc.commitShares)
//}
//
//func (lc *LogCert) commitBallot() int {
//	lc.mu.Lock()
//	defer lc.mu.Unlock()
//	return len(lc.commits)
//}

type RequestMsg struct {
	Operator  []byte
	Timestamp int64
	ClientId  int64
}

//type PrePrepareMsg struct {
//	View   int64
//	Seq    int64
//	Digest []byte
//	NodeId int64
//	PrimaryNodeId int64
//	TxNum int64
//}

type SendingMsg struct {
	PrimaryNodeId int64
	CommitBlockIndex int64
	CommitBlockTxNum int64
	Block *Block
}

type SendingReturnMsg struct{
	PrepareBlockIndex    int64
	CommittedBlockIndex  int64
	NewDuplicatedReqs []*duplicatedReqUnit
	NodeId int64
}

type PrepareMsg struct {
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
	TxNum int64
	logIndex int64
}

type duplicatedReqUnit struct{
	DuplicatingNodeId int64
	TxNum int64
	Digest []byte
	Sign []byte
}

type Block struct{
	//duplicatedReqs是所有已经被可靠广播的请求，收集自所有节点，用于排序
	DuplicatedReqs []*duplicatedReqUnit
	BlockIndex int64
	Committed bool
	TxNum int64
	//duplicatedReqsJson string
	//所有进入第二阶段要确认的区块
	//refBlock []int64
}


type DuplicateMsg struct {
	Seq    int64
	Digest []byte
	NodeId int64
	DuplicatorNodeId int64
	TxNum int64
	logIndex int64
}

type PrepareConfirmMsg struct {
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
	LogIndex int64
}

type DuplicateConfirmMsg struct {
	Seq    int64
	Digest []byte
	NodeId int64
	DuplicatorNodeId int64
	LogIndex int64
}

type CommitMsg struct {
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
	TxNum int64
	LogIndex int64
}

type CommitConfirmMsg struct {
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
	LogIndex int64
}

type ReplyMsg struct {
	Seq       int64
	Timestamp int64
	ClientId  int64
	NodeId    int64
	Result    []byte
}

type RequestArgs struct {
	Req  *RequestMsg
	Sign []byte
	TxNum int
}

type RequestReply struct {
	Seq int64
	Ok  bool
}

type PrepareArgs struct {
	Msg     *PrepareMsg
	Sign    []byte
	ReqArgs *RequestArgs
}

type SendingArgs struct {
	Msg     *SendingMsg
	Digest  []byte
	Sign    []byte
	//Block   *Block
}

type SendingReturnArgs struct {
	Msg     *SendingReturnMsg
	Digest  []byte
	Sign    []byte
}

type DuplicateArgs struct {
	Msg     *DuplicateMsg
	Sign    []byte
	ReqArgs *RequestArgs
}

type PrepareConfirmArgs struct {
	Msg  *PrepareConfirmMsg
	Sign []byte
}

type DuplicateConfirmArgs struct {
	Msg  *DuplicateConfirmMsg
	Sign []byte
}

type DuplicateConfirmArgs2 struct {
	Args []*DuplicateConfirmArgs
}

type CommitArgs struct {
	Msg  *CommitMsg
	Sign []byte
}

type CommitConfirmArgs struct {
	Msg  *CommitConfirmMsg
	Sign []byte
}

type ReplyArgs struct {
	Msg  *ReplyMsg
	Sign []byte
}

type CloseCliCliArgs struct {
	ClientId int64
}

type TreeBroadcastMsg struct {
	Tree   *TreeNode
	Seq    int64
	Digest []byte
	NodeId int64
	DuplicatorNodeId int64
	TxNum  int64
}

type TreeBroadcastArgs struct {
	TreeBCMsgs *TreeBroadcastMsg
	ReqArgs *RequestArgs
	Digest []byte
	Sign   []byte
}

type TreeBroadcastReplyMsg struct {
	Ok  int64
}

type TreeBroadcastReplyArgs struct {
	OkMsg  *TreeBroadcastReplyMsg
	Digest []byte
	Sign   []byte
}

type TreeBCBackMsg struct {
	ReqDigest []byte
	Seq int64
	NodeId int64
}

type TreeBCBackArgs struct {
	Msg *TreeBCBackMsg
	Digest []byte
	Sign []byte
}

type TreeBCBackReplyArgs struct {
	Ok int64
}