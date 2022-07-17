package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/zlog"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State = int32

const (
	Follower State = iota
	Candidate
	Leader
)

//
var state2str = map[State]string{
	Follower:  "follower",
	Candidate: "candidate",
	Leader:    "leader",
}

const (
	intervalTime         = 20
	heartbeatTime        = 200
	electionTimeoutFrom  = 600
	electionTimeoutRange = 200
	leaderTimeout        = 2000
	retryRpc             = 500
)

func GetRandomElapsedTime() int {
	return electionTimeoutFrom + rand.Intn(electionTimeoutRange)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state    State
	leaderId int
	applyCh  chan ApplyMsg

	currentTerm int
	votedFor    int
	log         []LogEntry
	leaderLost  int32 // leader 是否超时

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	peerConnBmap int32

	rw        sync.RWMutex
	headIndex int
	snapshot  []byte
}

func (rf *Raft) stateStr() string {
	return state2str[atomic.LoadInt32(&rf.state)]
}

func (rf *Raft) logInfo() string {
	if zlog.ZLogLevel >= zlog.InfoLevel {
		return ""
	}
	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := rf.GetLog(lastLogIndex).Term
	return fmt.Sprintf("<%d,%d,%d>|<%d,%d,%d>|<%d,%d>| ",
		rf.GetMe(), rf.GetLeaderId(), rf.GetCurrentTerm(),
		lastLogTerm, lastLogIndex, rf.GetCommitIndex(),
		rf.GetHeadIndex(), rf.GetLastLogIndex()-rf.GetHeadIndex())
}

// ============================================ FIXME: 核心数据操作，读写锁封装

func (rf *Raft) GetMe() int {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.me
}

func (rf *Raft) GetLeaderId() int {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.leaderId
}

func (rf *Raft) SetLeaderId(leaderId int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.leaderId = leaderId
}

func (rf *Raft) GetVotedFor() int {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.votedFor
}

func (rf *Raft) SetVotedFor(votedFor int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.votedFor = votedFor
}

func (rf *Raft) GetCurrentTerm() int {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) SetCurrentTerm(currentTerm int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.currentTerm = currentTerm
}

func (rf *Raft) GetCommitIndex() int {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.commitIndex
}

func (rf *Raft) SetCommitIndex(commitIndex int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.commitIndex = commitIndex
}

func (rf *Raft) GetLastApplied() int {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.lastApplied
}

func (rf *Raft) SetLastApplied(lastApplied int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.lastApplied = lastApplied
}

func (rf *Raft) GetHeadIndex() int {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.headIndex
}

func (rf *Raft) SetHeadIndex(headIndex int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.headIndex = headIndex
}

func (rf *Raft) GetSnapshot() []byte {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.snapshot
}

func (rf *Raft) SetSnapshot(snapshot []byte) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.snapshot = snapshot
}

// ================================== FIXME: 读写日志

func (rf *Raft) GetLastLogIndex() int {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.headIndex + len(rf.log) - 1
}

func (rf *Raft) GetLog(index int) LogEntry {
	rf.rw.RLock()
	defer rf.rw.RUnlock()

	memIndex := index - rf.headIndex
	if memIndex < 0 {
		zlog.Warn("index=%d, headIndex=%d, memIndex=%d", index, rf.headIndex, memIndex)
	}
	return rf.log[memIndex]
}

func (rf *Raft) getLogs(left, right int) []LogEntry {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	memLeft, memRight := left-rf.headIndex, right-rf.headIndex
	return rf.log[memLeft:memRight]
}

func (rf *Raft) getAllLog() []LogEntry {
	rf.rw.RLock()
	defer rf.rw.RUnlock()
	return rf.log
}

// >= index 的日志都舍弃
func (rf *Raft) truncateAt(index int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	memIndex := index - rf.headIndex
	rf.log = rf.log[:memIndex]
}

func (rf *Raft) appendLog(entries ...LogEntry) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) TrimLog(index int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	memIndex := index - rf.headIndex
	rf.log = rf.log[memIndex:]
	rf.headIndex = index
}

func (rf *Raft) ClearLog(headIndex, headTerm int) {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Term: headTerm})
	rf.headIndex = headIndex
}

// ============================================ FIXME: 日志操作，后期封装

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.GetCurrentTerm()
	if atomic.LoadInt32(&rf.state) == Leader {
		isleader = true
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	zlog.Debug(rf.logInfo()+"persist, votedFor=%d, LastLogIndex=%d",
		rf.GetVotedFor(), rf.GetLastLogIndex())

	e.Encode(rf.GetCurrentTerm())
	e.Encode(rf.GetVotedFor())
	// e.Encode(rf.getCommitIndex())
	// e.Encode(rf.lastApplied)
	e.Encode(rf.getAllLog())
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	var currentTerm int
	var votedFor int
	var log []LogEntry

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		zlog.Error(rf.logInfo()+"read persist error.", rf.GetMe(), rf.GetCurrentTerm(), rf.GetLeaderId())
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.SetCurrentTerm(currentTerm)
	rf.SetVotedFor(votedFor)
	rf.log = log

	zlog.Debug(rf.logInfo()+"read persist, votedFor=%d, LastLogIndex=%d",
		rf.GetVotedFor(), rf.GetLastLogIndex())
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// Previously, this lab recommended that you implement a function called CondInstallSnapshot
	// to avoid the requirement that snapshots and log entries sent on applyCh are coordinated.
	// This vestigal API interface remains, but you are discouraged from implementing it:
	// instead, we suggest that you simply have it return true.
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	// ! 异步执行, 否则会死锁，但异步会产生bug，尝试使用判断解决
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if index <= rf.GetHeadIndex() {
			return
		}
		rf.SetSnapshot(snapshot)
		rf.TrimLog(index)
		zlog.Debug(rf.logInfo()+"snapshot, index=%d", index)
	}()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// TODO: 先不管效率，加锁保护VoteFor变量
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.GetCurrentTerm()
	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := rf.GetLog(lastLogIndex).Term

	// 新的任期重置投票权
	if rf.GetCurrentTerm() < args.Term {
		rf.SetCurrentTerm(args.Term)
		rf.SetVotedFor(-1)
		atomic.StoreInt32(&rf.state, Follower) // 如果是leader或candidate重新变回followe
		// TODO 请求投票如果有更大任期则不能重置超时！！！
		// atomic.StoreInt32(&rf.leaderLost, 0)   // 重置超时flag
	}

	if rf.GetCurrentTerm() == args.Term &&
		(rf.GetVotedFor() == -1 || rf.GetVotedFor() == args.CandidateId) &&
		(lastLogTerm < args.LastLogTerm ||
			lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
		zlog.Debug(rf.logInfo()+"vote for %d", args.CandidateId)
		reply.VoteGranted = true
		rf.SetVotedFor(args.CandidateId)
	} else {
		reply.VoteGranted = false
		zlog.Debug(rf.logInfo()+"reject vote for %d", args.CandidateId)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.GetCurrentTerm()
	if atomic.LoadInt32(&rf.state) == Leader {
		isLeader = true
		rf.appendLog(LogEntry{
			Term:    rf.GetCurrentTerm(),
			Command: command,
		})
		index = rf.GetLastLogIndex()
		zlog.Debug(rf.logInfo()+"new log, term=%d, index=%d", term, index)
		rf.nextIndex[rf.GetMe()] = index + 1
		rf.matchIndex[rf.GetMe()] = index

		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	zlog.Debug(rf.logInfo() + "killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 如果已经是leader，则不运行超时机制，睡眠一个心跳时间
		if atomic.LoadInt32(&rf.state) != Leader {
			time.Sleep(heartbeatTime * time.Millisecond)
		}

		// 先将失效设置为1，leader的心跳包会将该值在检测期间重新置为 0
		atomic.StoreInt32(&rf.leaderLost, 1)

		// 失效时间在[electionTimeout, 2 * electionTimeout)间随机
		elapsedTime := GetRandomElapsedTime()
		time.Sleep(time.Duration(elapsedTime) * time.Millisecond)

		// 如果超时且不是Leader且参加选举
		if atomic.LoadInt32(&rf.leaderLost) == 1 && atomic.LoadInt32(&rf.state) != Leader {
			zlog.Debug(rf.logInfo()+"elapsedTime=%d (ms)", elapsedTime)
			rf.elect()
		}
	}
}

// 选举
func (rf *Raft) elect() {
	zlog.Debug(rf.logInfo()+"state:%s=>candidate, elect", rf.stateStr())

	args := func() *RequestVoteArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.SetCurrentTerm(rf.GetCurrentTerm() + 1) // 自增自己的当前term
		atomic.StoreInt32(&rf.state, Candidate)    // 身份先变为candidate
		rf.SetLeaderId(-1)                         // 无主状态
		rf.SetVotedFor(rf.GetMe())                 // 竞选获得票数，自己会先给自己投一票，若其他人请求投票会失败

		lastLogIndex := rf.GetLastLogIndex()
		lastLogTerm := rf.GetLog(lastLogIndex).Term
		return &RequestVoteArgs{
			Term:         rf.GetCurrentTerm(),
			CandidateId:  rf.GetMe(),
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
	}()
	// 选举票号统计，1为自己给自己投的票
	ballot := 1
	// 向所有peer发送请求投票rpc
	for server := range rf.peers {
		// 排除自身
		if server == rf.GetMe() {
			continue
		}
		// 并发请求投票，成为leader的逻辑也在此处理
		server1 := server
		go func() {
			// TODO 是否需要判断下已经成为leader而不用发送请求投票信息？
			zlog.Debug(rf.logInfo()+"request vote %d, start",
				server1)

			reply := &RequestVoteReply{}
			before := time.Now()
			ok := rf.sendRequestVote(server1, args, reply)
			take := time.Since(before)
			zlog.Debug(rf.logInfo()+"request vote %d, ok=%v, take=%v", server1, ok, take)
			if !ok {
				// TODO 投票请求重试
				return
			}
			// 计票
			rf.ballotCount(server1, &ballot, args, reply)
		}()
	}
}

func (rf *Raft) ballotCount(server int, ballot *int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 超时后过期的消息
	if args.Term != rf.GetCurrentTerm() {
		zlog.Debug(rf.logInfo()+"request vote %d, ok but expire, args.Term=%d, rf.getCurrentTerm()=%d",
			server, args.Term, rf.GetCurrentTerm())
		return
	}

	if !reply.VoteGranted {
		// 如果竞选的任期落后，则更新本节点的term，终止竞选
		if rf.GetCurrentTerm() < reply.Term {
			zlog.Debug(rf.logInfo()+"state:%s=>follower, higher term from %d",
				rf.stateStr(), server)
			atomic.StoreInt32(&rf.state, Follower)
			rf.SetCurrentTerm(reply.Term)
			rf.SetLeaderId(-1)
			rf.SetVotedFor(-1)
		}
		return
	}
	// 增加票数
	*ballot++
	zlog.Debug(rf.logInfo()+"request vote %d, peer.num=%d, current ballot=%d",
		server, len(rf.peers), *ballot)
	// 须获得半数以上的投票才能成为leader
	if *ballot*2 <= len(rf.peers) {
		return
	}
	// 如果状态已不是candidate，则无法变为leader
	if atomic.LoadInt32(&rf.state) == Candidate {
		// 本节点成为 leader
		zlog.Debug(rf.logInfo()+"state:%s=>leader", rf.stateStr())
		atomic.StoreInt32(&rf.state, Leader)
		rf.SetLeaderId(rf.GetMe())
		nextIndex := rf.GetLastLogIndex() + 1
		for i := range rf.nextIndex {
			rf.nextIndex[i] = nextIndex
			rf.matchIndex[i] = 0
		}
		// go rf.Start(rf.GetMe())
		go rf.timingHeartbeatForAll()
		go rf.appendEntriesForAll()
	}
}

// 发送心跳
func (rf *Raft) timingHeartbeatForAll() {

	zlog.Debug(rf.logInfo() + "timingHeartbeatForAll")
	half := len(rf.peers) / 2
	atomic.StoreInt32(&rf.peerConnBmap, 0)

	for !rf.killed() && atomic.LoadInt32(&rf.state) == Leader {
		peerConnBmap1 := int32(0)
		for t := 0; t < leaderTimeout; t += heartbeatTime {
			peerConnBmap1 |= atomic.LoadInt32(&rf.peerConnBmap)
			for server := range rf.peers {
				if server == rf.GetMe() {
					continue
				}
				// 如果heartbeatTime时间内未发送rpc，则发送心跳
				if ((atomic.LoadInt32(&rf.peerConnBmap) >> server) & 1) == 0 {
					go rf.heartbeatForOne(server)
				}
			}
			// 定时发送心跳
			atomic.StoreInt32(&rf.peerConnBmap, 0)
			time.Sleep(heartbeatTime * time.Millisecond)
		}

		// 每 leaderTimeout 时间统计连接状态，如果半数未连接则退出leader状态
		connectCount := 0
		for peerConnBmap1 != 0 {
			peerConnBmap1 &= (peerConnBmap1 - 1)
			connectCount++
		}
		if connectCount >= half {
			continue
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果连接数少于半数，则退化为 follower
		zlog.Debug(rf.logInfo()+"leader timeout, connect count=%d(<%d), state:%s=>follower",
			connectCount, half, rf.stateStr())
		atomic.StoreInt32(&rf.state, Follower)
		rf.SetLeaderId(-1)
		return
	}
}

func (rf *Raft) heartbeatForOne(server int) {
	args, stop := func() (*AppendEntriesArgs, bool) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.killed() || atomic.LoadInt32(&rf.state) != Leader {
			return nil, true
		}
		return &AppendEntriesArgs{
			Term:         rf.GetCurrentTerm(),
			LeaderId:     rf.GetMe(),
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
		}, false
	}()

	if stop {
		return
	}

	zlog.Debug(rf.logInfo()+"heartbeat to %d, start", server)

	reply := &AppendEntriesReply{}
	before := time.Now()
	ok := rf.sendAppendEntries(server, args, reply)
	take := time.Since(before)

	zlog.Debug(rf.logInfo()+"heartbeat to %d, ok=%v, take=%v", server, ok, take)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || atomic.LoadInt32(&rf.state) != Leader {
		return
	}

	// 超时后过期的回复消息
	if args.Term < rf.GetCurrentTerm() {
		zlog.Debug(rf.logInfo()+"heartbeat to %d, ok but expire, args.Term=%d, rf.getCurrentTerm()=%d, take=%v",
			server, args.Term, rf.GetCurrentTerm(), take)
		return
	}

	// 保持连接置位
	atomic.StoreInt32(&rf.peerConnBmap, atomic.LoadInt32(&rf.peerConnBmap)|(1<<server))

	if rf.GetCurrentTerm() < reply.Term {
		zlog.Debug(rf.logInfo()+"heartbeat to %d, higher term, state:%s=>follower", server, rf.stateStr())
		rf.SetCurrentTerm(reply.Term)
		atomic.StoreInt32(&rf.state, Follower)
		rf.SetLeaderId(-1)
		return
	}
}

func (rf *Raft) appendEntriesForAll() {
	zlog.Debug(rf.logInfo() + "appendEntriesForAll")
	for server := range rf.peers {
		if server == rf.GetMe() {
			continue
		}
		go rf.timingAppendEntriesForOne(server)
	}
}

func (rf *Raft) getArgs(server int, logMatched bool, nEntriesCopy int) (
	appendEntriesArgs *AppendEntriesArgs, installSnapshotArgs *InstallSnapshotArgs) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || atomic.LoadInt32(&rf.state) != Leader {
		return
	}

	startIndex := rf.nextIndex[server]

	// 如果 nextIndex 在快照中，则发送快照
	if startIndex <= rf.GetHeadIndex() {
		installSnapshotArgs = &InstallSnapshotArgs{
			Term:              rf.GetCurrentTerm(),
			LeaderId:          rf.GetMe(),
			LastIncludedIndex: rf.GetHeadIndex(),
			LastIncludedTerm:  rf.GetLog(rf.GetHeadIndex()).Term,
			Snapshot:          rf.GetSnapshot(),
		}
		return
	}

	if logMatched {
		// 指数递增拷贝
		// nEntriesCopy = MinInt(nEntriesCopy*nEntriesCopy+1, rf.getLastLogIndex() + 1-startIndex)
		// 全部取出拷贝
		nEntriesCopy = rf.GetLastLogIndex() + 1 - startIndex
	} else {
		nEntriesCopy = MinInt(1, rf.GetLastLogIndex()+1-startIndex)
	}

	appendEntriesArgs = &AppendEntriesArgs{
		Term:         rf.GetCurrentTerm(),
		LeaderId:     rf.GetMe(),
		PrevLogIndex: startIndex - 1,
		PrevLogTerm:  rf.GetLog(startIndex - 1).Term,
		Entries:      rf.getLogs(startIndex, startIndex+nEntriesCopy),
		LeaderCommit: rf.GetCommitIndex(),
	}
	return
}

func (rf *Raft) timingAppendEntriesForOne(server int) {
	zlog.Debug(rf.logInfo()+"timingAppendEntriesForOne, server=%d",
		server)
	logMatched := false
	nEntriesCopy := 0
	commitIndex := 0

	// TODO: 串行发送rpc，并行会遇到state不是leader却依然发送和处理信息的情况, 也存在rpc幂等性问题
	for !rf.killed() && atomic.LoadInt32(&rf.state) == Leader {

		// 前一个rpc发送成功，立即判断是否要发下一个，同时达到心跳时间发送一个必要rpc
		for !rf.killed() && atomic.LoadInt32(&rf.state) == Leader {
			// for sumTime := 0; sumTime < heartbeatTime; sumTime += intervalTime {
			// 检查是否已不是leader或被kill
			needSend := func() bool {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 有新的日志需要同步或有更新的commitIndex
				return rf.GetLastLogIndex() >= rf.nextIndex[server] || commitIndex != rf.GetCommitIndex()
			}()
			if needSend {
				break
			}
			time.Sleep(intervalTime * time.Millisecond)
		}

		appendEntriesArgs, installSnapshotArgs := rf.getArgs(server, logMatched, nEntriesCopy)

		if appendEntriesArgs != nil {
			rf.appendEntriesForOne(server, appendEntriesArgs, &logMatched, &commitIndex)
			nEntriesCopy = len(appendEntriesArgs.Entries)
		} else if installSnapshotArgs != nil {
			rf.installSnapshotForOne(server, installSnapshotArgs, &logMatched, &commitIndex)
		} else {
			// rf.killed() || atomic.LoadInt32(&rf.state) != Leader
			return
		}
	}
}

func (rf *Raft) appendEntriesForOne(server int, args *AppendEntriesArgs, logMatched *bool, commitIndex *int) {

	replyCh := make(chan *AppendEntriesReply)
	before := time.Now()
	go func() {
		received := int32(0)
		count := int32(0)
		for {
			if atomic.LoadInt32(&received) == 1 {
				return
			}
			go func() {
				zlog.Debug(rf.logInfo()+"append entries to %d, entries=(%d, %d], PrevLogIndex=%d, PrevLogTerm=%d, LeaderCommit=%d, sendCount=%d",
					server, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, atomic.LoadInt32(&count))
				atomic.AddInt32(&count, 1)
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				// 原子操作，保证只有一个 reply 进入 通道
				if ok && atomic.SwapInt32(&received, 1) == 0 {
					replyCh <- reply
				}
			}()
			// 每隔 retryRpc 进行重发，直至取消
			time.Sleep(retryRpc * time.Millisecond)
		}
	}()

	// 等待第一个返回的 rpc，其他的舍弃
	reply := <-replyCh
	take := time.Since(before)
	zlog.Debug(rf.logInfo()+"append entries to %d ok, take=%v", server, take)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || atomic.LoadInt32(&rf.state) != Leader {
		return
	}

	// 超时后过期的回复消息
	if args.Term < rf.GetCurrentTerm() {
		zlog.Debug(rf.logInfo()+"append entries to %d, ok but expire, args.Term=%d, rf.getCurrentTerm()=%d, take=%v",
			server, args.Term, rf.GetCurrentTerm(), take)
		return
	}

	// 保持连接置位
	atomic.StoreInt32(&rf.peerConnBmap, atomic.LoadInt32(&rf.peerConnBmap)|(1<<server))

	// 遇到更高任期，成为follower
	if rf.GetCurrentTerm() < reply.Term {
		zlog.Debug(rf.logInfo()+"append entries to %d, higher term, state:%s=>follower",
			server, rf.stateStr())
		rf.SetCurrentTerm(reply.Term)
		atomic.StoreInt32(&rf.state, Follower)
		rf.SetLeaderId(-1)
		return
	}

	// 如果不成功，说明PrevLogIndex不匹配
	if !reply.Success {

		zlog.Debug(rf.logInfo()+"append entries to %d, no match, PrevLogIndex=%d, PrevLogTerm=%d, reply.LogIndex=%d, reply.term=%d",
			server, args.PrevLogIndex, args.PrevLogTerm, reply.LogIndex, reply.Term)

		*logMatched = rf.matchNextIndex(server, args.PrevLogIndex, args.PrevLogTerm, reply.LogIndex, reply.Term)
		return
	}

	if len(args.Entries) != 0 {
		// 复制到副本成功
		rf.matchIndex[server] = MaxInt(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}

	zlog.Debug(rf.logInfo()+"append entries to %d, match ok, copy entries=(%d, %d], match.index=%d, next.index=%d",
		server, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), rf.matchIndex[server], rf.nextIndex[server])

	// 本server commitIndex的值
	*commitIndex = MinInt(args.LeaderCommit, rf.matchIndex[server])

	// 推进leader的commit
	rf.advanceLeaderCommit()
	// follower和leader日志匹配成功，可以发送后续日志
	*logMatched = true
}

func (rf *Raft) installSnapshotForOne(server int, args *InstallSnapshotArgs, logMatched *bool, commitIndex *int) {

	replyCh := make(chan *InstallSnapshotReply)
	before := time.Now()
	go func() {
		received := int32(0)
		count := int32(0)
		for {
			if atomic.LoadInt32(&received) == 1 {
				return
			}
			go func() {
				zlog.Debug(rf.logInfo()+"install snapshot for %d, LastIncludedIndex=%d, LastIncludedTerm=%d, sendCount=%d",
					server, args.LastIncludedIndex, args.LastIncludedTerm, atomic.LoadInt32(&count))
				atomic.AddInt32(&count, 1)
				reply := &InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(server, args, reply)
				// 原子操作，保证只有一个 reply 进入 通道
				if ok && atomic.SwapInt32(&received, 1) == 0 {
					replyCh <- reply
				}
			}()
			// 每隔 retryRpc 进行重发，直至取消
			time.Sleep(retryRpc * time.Millisecond)
		}
	}()

	// 等待第一个返回的 rpc，其他的舍弃
	reply := <-replyCh
	take := time.Since(before)
	zlog.Debug(rf.logInfo()+"install snapshot for %d ok, take=%v", server, take)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || atomic.LoadInt32(&rf.state) != Leader {
		return
	}

	// 超时后过期的回复消息
	if args.Term < rf.GetCurrentTerm() {
		zlog.Debug(rf.logInfo()+"append entries to %d, ok but expire, args.Term=%d, rf.getCurrentTerm()=%d, take=%v",
			server, args.Term, rf.GetCurrentTerm(), take)
		return
	}

	// 保持连接置位
	atomic.StoreInt32(&rf.peerConnBmap, atomic.LoadInt32(&rf.peerConnBmap)|(1<<server))

	// 遇到更高任期，成为follower
	if rf.GetCurrentTerm() < reply.Term {
		zlog.Debug(rf.logInfo()+"append entries to %d, higher term, state:%s=>follower",
			server, rf.stateStr())
		rf.SetCurrentTerm(reply.Term)
		atomic.StoreInt32(&rf.state, Follower)
		rf.SetLeaderId(-1)
		return
	}

	*commitIndex = args.LastIncludedIndex
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	*logMatched = true
}

// 在互斥区中
func (rf *Raft) matchNextIndex(server, prevLogIndex, prevLogTerm, replyLogIndex, replyLogTerm int) bool {

	// ! 先判断是否在 headIndex 前
	if replyLogIndex < rf.GetHeadIndex() || rf.GetLog(replyLogIndex).Term < replyLogTerm {
		// 从前面试起
		rf.nextIndex[server] = replyLogIndex
		return false
	}
	if rf.GetLog(replyLogIndex).Term == replyLogTerm {
		// 刚好匹配，从下一个开始
		rf.nextIndex[server] = replyLogIndex + 1
		return true
	}
	// ! 二分优化快速查找, 注意索引
	left, right := MaxInt(rf.GetHeadIndex(), rf.matchIndex[server]), replyLogIndex
	for left < right {
		mid := left + (right-left)/2
		zlog.Debug(rf.logInfo()+"append entries to %d, for match, rf.getLog(%d).Term=%d, reply.Term=%d",
			server, mid, rf.GetLog(mid).Term, replyLogTerm)
		if rf.GetLog(mid).Term <= replyLogTerm {
			left = mid + 1
		} else {
			right = mid
		}
	}
	zlog.Debug(rf.logInfo()+"append entries to %d, for match, rf.getLog(%d).Term=%d, reply.Term=%d",
		server, left, rf.GetLog(left).Term, replyLogTerm)

	// 最近位置索引大一 (left - 1) + 1
	rf.nextIndex[server] = left
	return false
}

// 在互斥区中
func (rf *Raft) advanceLeaderCommit() {
	// 判断是否需要递增 commitIndex，排序找出各个匹配的中位值就是半数以上都接受的日志
	indexes := make([]int, 0, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.GetMe() {
			indexes = append(indexes, rf.matchIndex[i])
		}
	}
	sort.Ints(indexes)
	newCommitIndex := indexes[len(indexes)-len(rf.peers)/2]
	// 相同任期才允许apply，避免被commit日志被覆盖的情况
	if newCommitIndex <= rf.GetHeadIndex() {
		zlog.Debug(rf.logInfo()+"newCommitIndex=%d, indexes=%d", newCommitIndex, indexes)
	}

	// ! 条件不能颠倒，必须保证 newCommitIndex > commitIndex >= headIndex
	if newCommitIndex > rf.GetCommitIndex() && rf.GetLog(newCommitIndex).Term == rf.GetCurrentTerm() {
		// apply
		zlog.Debug(rf.logInfo()+"leader apply log [%d, %d]", rf.GetCommitIndex()+1, newCommitIndex)
		rf.applyLogEntries(rf.GetCommitIndex()+1, newCommitIndex)
		rf.SetCommitIndex(newCommitIndex)
	}
}

type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones

	PrevLogTerm int        // term of prevLogIndex entry
	Entries     []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)

	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term     int  // currentTerm, for leader to update itself
	LogIndex int  // 换主后，为leader快速定位匹配日志
	Success  bool // success true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.GetCurrentTerm() {
		reply.Term = rf.GetCurrentTerm()
		reply.Success = false
		return
	}

	atomic.StoreInt32(&rf.state, Follower) // 无论原来状态是什么，状态更新为follower
	atomic.StoreInt32(&rf.leaderLost, 0)   // 重置超时flag
	rf.SetCurrentTerm(args.Term)           // 新的Term应该更高

	// 新的leader产生
	if args.LeaderId != rf.GetLeaderId() {
		zlog.Debug(rf.logInfo()+"state:%s=>follower, new leader %d",
			rf.stateStr(), args.LeaderId)
		rf.SetLeaderId(args.LeaderId)
	}

	reply.Success = true
	reply.Term = rf.GetCurrentTerm()

	// leader 的心跳消息，直接返回
	if args.PrevLogIndex < 0 {
		zlog.Debug(rf.logInfo()+"heartbeat from %d, reply.Term=%d",
			args.LeaderId, reply.Term)
		reply.LogIndex = -100
		return
	}

	// 可能因超时而过期的消息，舍弃
	if args.PrevLogIndex < rf.GetHeadIndex() || args.PrevLogIndex < rf.GetCommitIndex() {
		return
	}

	// 相同日志未直接定位到，需多轮交互
	if !rf.foundSameLog(args, reply) {
		return
	}

	// 对日志进行操作
	rf.updateEntries(args, reply)
}

// 在临界区中
func (rf *Raft) foundSameLog(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.GetLastLogIndex() < args.PrevLogIndex {
		zlog.Debug(rf.logInfo()+"from %d, no found same log, LastLogIndex(%d) < args.PrevLogIndex(%d)",
			args.LeaderId, rf.GetLastLogIndex(), args.PrevLogIndex)
	} else if rf.GetLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		zlog.Debug(rf.logInfo()+"from %d, no found same log, rf.getLog(%d).Term=%d != args.PrevLogTerm=%d",
			args.LeaderId, args.PrevLogIndex, rf.GetLog(args.PrevLogIndex).Term, args.PrevLogTerm)
	} else {
		return true
	}

	index := MinInt(args.PrevLogIndex, rf.GetLastLogIndex())
	if rf.GetLog(index).Term > args.PrevLogTerm {
		// 如果term不等，则采用二分查找找到最近匹配的日志索引
		left, right := rf.GetCommitIndex(), index
		for left < right {
			mid := left + (right-left)/2
			zlog.Debug(rf.logInfo()+"from %d, no found same log, rf.getLog(%d).Term=%d, args.PrevLogTerm=%d",
				args.LeaderId, mid, rf.GetLog(mid).Term, args.PrevLogTerm)
			if rf.GetLog(mid).Term <= args.PrevLogTerm {
				left = mid + 1
			} else {
				right = mid
			}
		}
		index = left - 1
	}

	zlog.Debug(rf.logInfo()+"from %d, no found same log, rf.getLog(%d).Term=%d, args.PrevLogTerm=%d",
		args.LeaderId, index, rf.GetLog(index).Term, args.PrevLogTerm)

	reply.Term = rf.GetLog(index).Term
	reply.LogIndex = index
	reply.Success = false
	return false
}

// 在临界区中
func (rf *Raft) updateEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.GetLastLogIndex() > args.PrevLogIndex {

		// 后续日志已经匹配，过期的消息, len(args.Entries) > 0 通常是 true
		// 在接受方保证幂等性
		if len(args.Entries) > 0 && rf.GetLog(args.PrevLogIndex+1).Term == args.Entries[0].Term {
			return
		}
		// 截断后面的日志
		zlog.Debug(rf.logInfo()+"truncate log at %d", args.PrevLogIndex+1)
		rf.truncateAt(args.PrevLogIndex + 1)
	}

	// 添加日志
	rf.appendLog(args.Entries...)
	// FIXME: 日志变动后及时更新最新日志索引，否则bug，修改代码

	// 在log变更时进行持久化
	rf.persist()

	// 推进commit和apply
	oldCommitIndex := rf.GetCommitIndex()
	rf.SetCommitIndex(MinInt(args.LeaderCommit, rf.GetLastLogIndex()))
	rf.applyLogEntries(oldCommitIndex+1, rf.GetCommitIndex())
	rf.SetLastApplied(rf.GetCommitIndex())

	zlog.Debug(rf.logInfo()+"append %d entries: <%d, %d>, commit: <%d, %d>",
		len(args.Entries), args.PrevLogIndex, rf.GetLastLogIndex(), oldCommitIndex, rf.GetCommitIndex())
}

// 在临界区中
func (rf *Raft) applyLogEntries(left, right int) {
	for i := left; i <= right; i++ {
		zlog.Debug(rf.logInfo()+"apply, commandIndex=%d", i)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.GetLog(i).Command,
			CommandIndex: i,
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Snapshot          []byte // 这里一次性发送过去
	// Offset            int    // byte offset where chunk is positioned in the snapshot file
	// Data              []byte // raw bytes of the snapshot chunk, starting at offset
	// Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.GetCurrentTerm()
	if args.Term < rf.GetCurrentTerm() {
		return
	}

	atomic.StoreInt32(&rf.state, Follower) // 无论原来状态是什么，状态更新为follower
	atomic.StoreInt32(&rf.leaderLost, 0)   // 重置超时flag
	rf.SetCurrentTerm(args.Term)           // 新的Term应该更高

	// 新的leader产生
	if args.LeaderId != rf.GetLeaderId() {
		zlog.Debug(rf.logInfo()+"state:%s=>follower, new leader %d",
			rf.stateStr(), args.LeaderId)
		rf.SetLeaderId(args.LeaderId)
	}

	// 快照可能因为网络延时而过期，丢弃
	// 确保幂等性，相同的快照不会安装两次
	if args.LastIncludedIndex <= rf.GetHeadIndex() {
		return
	}

	// FIXME: 这里预设 commitIndex 前的日志一定是一致相同的，其后才会出现日志不匹配情况
	// headIndex <= lastApplied <= commitIndex <= lastLogIndex
	//     ---- args.LastIncludedIndex ----
	if rf.GetLastApplied() < args.LastIncludedIndex {
		rf.SetLastApplied(args.LastIncludedIndex)
		if rf.GetCommitIndex() < args.LastIncludedIndex {
			rf.SetCommitIndex(args.LastIncludedIndex)
			// 快照完全超过当前节点的日志，或日志不匹配，则清除整个日志
			// 否则后续日志相同，保留
			if rf.GetLastLogIndex() < args.LastIncludedIndex ||
				rf.GetLog(args.LastIncludedIndex).Term != args.LastIncludedTerm {
				// 清除整个日志，同时设置 headIndex 和 headTerm
				rf.ClearLog(args.LastIncludedIndex, args.LastIncludedTerm)
			}
		}
	}
	rf.TrimLog(args.LastIncludedIndex)
	rf.SetSnapshot(args.Snapshot)

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// rf := &Raft{}
	// rf.peers = peers
	// rf.persister = persister
	// rf.setMe(me)

	// Your initialization code here (2A, 2B, 2C).

	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       Follower,
		leaderId:    -1,
		applyCh:     applyCh,
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0}}, // 初始存一个默认entry，索引和任期为0
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	zlog.Debug(rf.logInfo()+"make raft, peers.num:%d", len(rf.peers))

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}
