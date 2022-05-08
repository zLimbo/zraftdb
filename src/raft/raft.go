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
	intervalTime         = 40
	heartbeatTime        = 80
	electionTimeoutFrom  = 600
	electionTimeoutRange = 200
	leaderTimeout        = 1000
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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
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
	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| persist, votedFor=%d, log.len=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		rf.votedFor, len(rf.log))

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// e.Encode(rf.commitIndex)
	// e.Encode(rf.lastApplied)
	e.Encode(rf.log)
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
		zlog.Error("%d|%2d|%d|%d|<%d,%d>| read persist error.", rf.me, rf.currentTerm, rf.leaderId)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| read persist, votedFor=%d, log.len=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		rf.votedFor, len(rf.log))
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

	reply.Term = rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	// 新的任期重置投票权
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		// 如果是leader或candidate重新变回followe
		atomic.StoreInt32(&rf.state, Follower)
	}

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLogTerm < args.LastLogTerm ||
			lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| vote for %d", rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| reject vote for %d", rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.CandidateId)
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

type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones

	PrevLogTerm int        // term of prevLogIndex entry
	Entries     []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)

	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	LogIndex int // 换主后，为leader快速定位匹配日志
	Success bool // success true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 新的leader产生
	if args.LeaderId != rf.leaderId {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| state:%s=>follower, new leader %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			state2str[atomic.LoadInt32(&rf.state)], args.LeaderId)
		rf.leaderId = args.LeaderId
	}

	atomic.StoreInt32(&rf.state, Follower) // 无论原来状态是什么，状态更新为follower
	atomic.StoreInt32(&rf.leaderLost, 0)   // 重置超时flag
	rf.currentTerm = args.Term             // 新的Term应该更高

	logMatched := true
	if len(rf.log) <= args.PrevLogIndex {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| len(rf.log) <= args.PrevLogIndex, %d <= %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			len(rf.log), args.PrevLogIndex)
		logMatched = false
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| prevLogIndex=%d, rf.log[args.PrevLogIndex].Term != args.PrevLogTerm, %d != %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		logMatched = false
	}

	if !logMatched {
		index := MinInt(args.PrevLogIndex, len(rf.log)-1)
		if rf.log[index].Term > args.PrevLogTerm {
			// 如果term不等，则采用二分查找找到最近匹配的日志索引
			left, right := 0, MinInt(args.PrevLogIndex, len(rf.log)-1)+1
			for left < right {
				mid := left + (right-left)/2
				zlog.Debug("%d|%2d|%d|%d|<%d,%d>| index=%d, rf.log[%d].Term=%d, args.PrevLogTerm=%d",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					mid, mid, rf.log[mid].Term, args.PrevLogTerm)
				if rf.log[mid].Term <= args.PrevLogTerm {
					left = mid + 1
				} else {
					right = mid
				}
			}
			index = left - 1
		}

		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| index=%d, rf.log[%d].Term=%d, args.PrevLogTerm=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			index, index, rf.log[index].Term, args.PrevLogTerm)

		reply.Term = rf.log[index].Term
		reply.LogIndex = index
		reply.Success = false
		return
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	// 可能接受到重发或更早的请求，只处理后发的请求
	if args.PrevLogIndex < rf.commitIndex {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| repeat or old rpc",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		return
	}

	// 截断后面的日志
	if len(rf.log) > args.PrevLogIndex+1 {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| truncate log at %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.PrevLogIndex)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	// 添加日志
	rf.log = append(rf.log, args.Entries...)

	//
	rf.persist()

	// 推进commit和apply
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = MinInt(args.LeaderCommit, len(rf.log)-1)
	rf.lastApplied = rf.commitIndex

	// apply
	rf.applyLogEntries(oldCommitIndex+1, rf.commitIndex)

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append %d entries: <%d, %d>, commit: <%d, %d>",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		len(args.Entries), args.PrevLogIndex,
		len(rf.log)-1, oldCommitIndex, rf.commitIndex)
}

func (rf *Raft) applyLogEntries(left, right int) {
	for i := left; i <= right; i++ {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| apply, commandIndex=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			i)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	term = rf.currentTerm
	if atomic.LoadInt32(&rf.state) == Leader {
		isLeader = true
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		index = len(rf.log) - 1
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| new log, term=%d, index=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			term, index)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

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

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| killed",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
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
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| elapsedTime=%d (ms)",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term, elapsedTime)
			rf.elect()
		}
	}
}

// 选举
func (rf *Raft) elect() {
	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| state:%s=>candidate, elect",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		state2str[atomic.LoadInt32(&rf.state)])

	args := func() *RequestVoteArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.currentTerm += 1                     // 自增自己的当前term
		atomic.StoreInt32(&rf.state, Candidate) // 身份先变为candidate
		rf.leaderId = -1                        // 无主状态
		rf.votedFor = rf.me                     // 竞选获得票数，自己会先给自己投一票，若其他人请求投票会失败

		return &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
	}()

	// 选举票号统计，1为自己给自己投的票
	var ballot int32 = 1

	// 向所有peer发送请求投票rpc
	for server := range rf.peers {
		// 排除自身
		if server == rf.me {
			continue
		}
		// 并发请求投票，成为leader的逻辑也在此处理
		server1 := server
		go func() {
			// TODO 是否需要判断下已经成为leader而不用发送请求投票信息？
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| request vote %d", rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server1)
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(server1, args, reply); !ok {
				zlog.Debug("%d|%2d|%d|%d|<%d,%d>| rpc sendRequestVote failed=%d", rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server1)
				// TODO 投票请求重试
				return
			}
			if !reply.VoteGranted {
				// 如果竞选的任期落后，则更新本节点的term，终止竞选
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm < reply.Term {
						zlog.Debug("%d|%2d|%d|%d|<%d,%d>| state:%s=>follower, higher term from %d",
							rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
							state2str[atomic.LoadInt32(&rf.state)], server1)
						atomic.StoreInt32(&rf.state, Follower)
						rf.currentTerm = reply.Term
						rf.leaderId = -1
						rf.votedFor = -1
						return
					}
				}()
				return
			}
			// 增加票数
			curBallot := atomic.AddInt32(&ballot, 1)
			// 须获得半数以上的投票才能成为leader
			if int(curBallot)*2 <= len(rf.peers) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 如果状态已不是candidate，则无法变为leader
			if atomic.LoadInt32(&rf.state) == Candidate {
				// 本节点成为 leader
				zlog.Debug("%d|%2d|%d|%d|<%d,%d>| state:%s=>leader",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					state2str[atomic.LoadInt32(&rf.state)])
				atomic.StoreInt32(&rf.state, Leader)
				rf.leaderId = rf.me
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				go rf.heartbeat()
			}
		}()
	}
}

// 发送心跳
func (rf *Raft) heartbeat() {

	keepConnect := make(map[int]bool, len(rf.peers))
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.timingSend(server, keepConnect)
	}

	for !rf.killed() && atomic.LoadInt32(&rf.state) == Leader {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				keepConnect[i] = false
			}
		}()
		time.Sleep(leaderTimeout * time.Millisecond)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			connectCount := 0
			for i := 0; i < len(rf.peers); i++ {
				if keepConnect[i] {
					connectCount++
				}
			}
			// 如果连接数少于半数，则退化为 follower
			if connectCount < len(rf.peers)/2 {
				zlog.Debug("%d|%2d|%d|%d|<%d,%d>| leader timeout, connect count=%d(<%d), state:%s=>follower",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					connectCount, len(rf.peers)/2, state2str[atomic.LoadInt32(&rf.state)])
				atomic.StoreInt32(&rf.state, Follower)
				rf.leaderId = -1
			}
		}()
	}
}

func (rf *Raft) timingSend(server int, keepConnect map[int]bool) {

	logMatched := false
	nEntriesCopy := 1 // TODO 初始为0会出问题
	nIdempotency := 0
	rpcFinish := make(chan int, 100)

	rpcFinish <- nIdempotency

	for !rf.killed() && atomic.LoadInt32(&rf.state) == Leader {
		// TODO: 并行发送rpc，会遇到state不是leader却依然发送和处理信息的情况, 也存在rpc幂等性问题

		go func() {
			args, idIdempotency, ok := func() (*AppendEntriesArgs, int, bool) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				startIndex := rf.nextIndex[server]

				if rf.killed() || atomic.LoadInt32(&rf.state) != Leader {
					return nil, nIdempotency, false
				}

				if startIndex <= 0 {
					return nil, nIdempotency, false
				}

				if logMatched {
					// 指数递增拷贝
					// nEntriesCopy = MinInt(nEntriesCopy*nEntriesCopy+1, len(rf.log)-startIndex)
					nEntriesCopy = len(rf.log) - startIndex
				} else {
					nEntriesCopy = MinInt(1, len(rf.log)-startIndex)
				}
				// nEntriesCopy = len(rf.log)-startIndex

				zlog.Debug("%d|%2d|%d|%d|<%d,%d>| set args to %d,  id=%d, PrevLogIndex=%d, rf.nextIndex[server]=%d",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server, nIdempotency, startIndex-1, rf.nextIndex[server])

				return &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: startIndex - 1,
					PrevLogTerm:  rf.log[startIndex-1].Term,
					Entries:      rf.log[startIndex : startIndex+nEntriesCopy],
					LeaderCommit: rf.commitIndex,
				}, nIdempotency, true
			}()

			if !ok {
				return
			}

			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| send to %d, PrevLogTerm=%d, send %d entries: <%d, %d>, commitIndex=%d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server, args.PrevLogTerm,
				len(args.Entries), args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)

			reply := &AppendEntriesReply{}
			before := time.Now()
			rpcOk := rf.sendAppendEntries(server, args, reply)
			take := time.Since(before)

			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d, id=%d, take=%v",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server, idIdempotency, take)

			nextId := -1
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !rpcOk {
					zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d, rpc sendAppendEntries failed, take=%v",
						rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
						server, take)
					logMatched = false
					nextId = nIdempotency
					return
				}

				keepConnect[server] = true

				if rf.currentTerm < reply.Term {
					zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d, higher term, state:%s=>follower, exit heartbeat",
						rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
						server, state2str[atomic.LoadInt32(&rf.state)])
					rf.currentTerm = reply.Term
					atomic.StoreInt32(&rf.state, Follower)
					rf.leaderId = -1
					return
				}

				// 如果已有当前rpc发送完成，则不进行后面的处理
				if idIdempotency != nIdempotency {
					return
				}
				nIdempotency++
				nextId = nIdempotency

				// zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d, id=%d",
				// 	rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				// 	server, idIdempotency)

				// 如果不成功，说明PrevLogIndex不匹配，递减nextIndex(或已经不是leader)
				if !reply.Success {

					zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d, log match unsuccessful: prev.term=%d prev.index=%d reply.term=%d, reply.index=%d",
						rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
						server, args.PrevLogTerm, args.PrevLogIndex, reply.Term, reply.LogIndex)

					// 这种情况说明该节点已经不是leader
					if args.PrevLogTerm < reply.Term {
						return
					}

					if args.PrevLogTerm == reply.Term {
						rf.nextIndex[server] = reply.LogIndex + 1
						logMatched = true
						// rf.nextIndex[server]--
					} else {
						logMatched = false
						// 二分优化快速查找
						left, right := 0, args.PrevLogIndex
						for left < right {
							mid := left + (right-left)/2

							zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d, index=%d, rf.log[%d].Term=%d, reply.Term=%d",
								rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
								server, mid, mid, rf.log[mid].Term, reply.Term)

							if rf.log[mid].Term <= reply.Term {
								left = mid + 1
							} else {
								right = mid
							}
						}
						zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d,  index=%d, rf.log[%d].Term=%d, reply.Term=%d",
							rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
							server, left, left, rf.log[left].Term, reply.Term)

						// 最近位置索引大一 (left - 1) + 1
						rf.nextIndex[server] = left
					}
					
					return
				}

				if len(args.Entries) == 0 {
					zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d, log match successful: prev.term=%d prev.index=%d",
						rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
						server, args.PrevLogTerm, args.PrevLogIndex)
					return
				}
				// 复制到副本成功
				rf.nextIndex[server] = MaxInt(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
				rf.matchIndex[server] = MaxInt(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				zlog.Debug("%d|%2d|%d|%d|<%d,%d>| response from %d, log copy successful: follower.index=%d",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server, rf.nextIndex[server]-1)

				// 判断是否需要递增 commitIndex，排序找出各个匹配的中位值就是半数以上都接受的日志
				indexes := make([]int, 0, len(rf.matchIndex)-1)
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						indexes = append(indexes, rf.matchIndex[i])
					}
				}
				sort.Ints(indexes)
				newCommitIndex := indexes[len(indexes)-len(rf.matchIndex)/2]
				// 相同任期才允许apply，避免被commit日志被覆盖的情况
				if rf.log[newCommitIndex].Term == rf.currentTerm && newCommitIndex > rf.commitIndex {
					oldCommitIndex := rf.commitIndex
					rf.commitIndex = newCommitIndex
					// apply
					rf.applyLogEntries(oldCommitIndex+1, newCommitIndex)
				}
				// follower和leader日志匹配成功，可以发送后续日志
				logMatched = true
			}()

			if nextId != -1 {
				// 发送结束
				rpcFinish <- nextId
			}
		}()

		select {
		case <-rpcFinish:
			// 前一个rpc发送成功，立即判断是否要发下一个，同时达到心跳时间发送心跳
			for sumTime := 0; sumTime < heartbeatTime; sumTime += intervalTime {
				// 检查是否已不是leader或被kill
				if rf.killed() || atomic.LoadInt32(&rf.state) != Leader {
					return
				}
				needSend := func() bool {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 有新的日志需要同步
					return len(rf.log)-1 >= rf.nextIndex[server]
				}()
				if needSend {
					break
				}
				time.Sleep(intervalTime * time.Millisecond)
				if sumTime+intervalTime >= heartbeatTime {
					zlog.Debug("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d",
						rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
						server)
				}
			}

		case <-time.After(2 * intervalTime * time.Millisecond):
			// 超时重发
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| timeout send to %d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server)
		}
	}
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
	// rf.me = me

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

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| make raft, peers:%v", rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		rf.peers)

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
