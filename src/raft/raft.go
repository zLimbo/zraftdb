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
	"log"
	"math/rand"
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

var state2str = map[State]string{
	Follower:  "follower",
	Candidate: "candidate",
	Leader:    "leader",
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

	currentTerm     int
	voteFor         int
	logs            []LogEntry
	electionTimeout int32 // leader 是否超时

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
	if rf.state == Leader {
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Panicln("read persist error.")
	} else {

		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.currentTerm = currentTerm
		rf.voteFor = votedFor
		rf.logs = logs
	}
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
	if rf.currentTerm <= args.Term &&
		(rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		rf.commitIndex <= args.LastLogIndex {
		zlog.Debug("%d | %d | %2d | vote for %d", rf.me, rf.currentTerm, rf.leaderId, args.CandidateId)
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		// 定时重置投票权
		go func() {
			elapsedTime := rand.Intn(150) + 150
			time.Sleep(time.Duration(elapsedTime) * time.Millisecond)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.voteFor = -1
		}()
	} else {
		reply.VoteGranted = false
		zlog.Debug("%d | %d | %2d | reject vote for %d", rf.me, rf.currentTerm, rf.leaderId, args.CandidateId)
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
	Success bool // success true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	reply.Success = true

	// TODO 暂时先处理心跳包

	// 新的leader产生
	if args.LeaderId != rf.leaderId {
		zlog.Debug("%d | %d | %2d | state: %s => follower, new leader %d",
			rf.me, rf.currentTerm, rf.leaderId, state2str[rf.state], args.LeaderId)
		rf.leaderId = args.LeaderId
		rf.state = Follower
		rf.currentTerm = args.Term // 新的Term应该更高
		rf.voteFor = -1
	}

	// 重置超时flag
	rf.electionTimeout = 0
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

	index = rf.commitIndex
	term = rf.currentTerm
	if rf.state == Leader {
		isLeader = true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 如果本节点是leader，则发送心跳包，每20ms发送一次
		if atomic.LoadInt32(&rf.state) == Leader {
			time.Sleep(20 * time.Millisecond)
			rf.heartbeat()
			continue
		}

		if atomic.LoadInt32(&rf.state) == Candidate {
			// candidate 随时可能成为leader，要及时进行heartbeat, 所以不能长时间睡眠
			time.Sleep(20 * time.Millisecond)
			continue
		}

		// 如果是follower，则检测leader是否失效，重新进行选举
		// 先将失效设置为1，leader的心跳包会将该值在检测期间重新置为 0
		atomic.StoreInt32(&rf.electionTimeout, 1)

		// 失效时间在[150ms,300ms)间随机
		elapsedTime := rand.Intn(150) + 150
		zlog.Debug("%d | %d | %2d | elapsedTime: %d (ms)", rf.me, rf.currentTerm, rf.leaderId, elapsedTime)
		time.Sleep(time.Duration(elapsedTime) * time.Millisecond)

		// 如果失效则参加选举
		if atomic.LoadInt32(&rf.electionTimeout) == 1 {
			rf.elect()
		}
	}
}

func (rf *Raft) heartbeat() {

	args := func() *AppendEntriesArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.logs) - 1,
			PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
			Entries:      []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}
	}()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		reply := &AppendEntriesReply{}
		server1 := server
		go func() {
			zlog.Debug("%d | %d | %2d | heartbeat %d", rf.me, rf.currentTerm, rf.leaderId, server1)
			ok := rf.sendAppendEntries(server1, args, reply)
			if !ok {
				// 10ms 后重发一次
				time.Sleep(10 * time.Millisecond)
				ok = rf.sendAppendEntries(server1, args, reply)
			}
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm < reply.Term {
					zlog.Debug("%d | %d | %2d | state: %s => follower, higher term from %d",
						rf.me, rf.currentTerm, rf.leaderId, state2str[rf.state], server1)
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.leaderId = -1
				}
			}
		}()
	}
}

func (rf *Raft) elect() {
	zlog.Debug("%d | %d | %2d | state: %s => candidate, elect",
		rf.me, rf.currentTerm, rf.leaderId, state2str[rf.state])

	args := func() *RequestVoteArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// 如果已投票，则不进行选举
		if rf.voteFor != -1 {
			return nil
		}

		rf.currentTerm += 1  // 自增自己的当前term
		rf.state = Candidate // 身份先变为candidate
		rf.leaderId = -1     // 无主状态
		rf.voteFor = rf.me   // 竞选获得票数，自己会先给自己投一票，若其他人请求投票会失败

		return &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
	}()

	// 已投票，不进行选举
	if args == nil {
		return
	}

	// 选举票号统计，1为自己给自己投的票
	var ballot int32 = 1

	// 候选人状态超时，则退回 follower 身份
	go func() {
		// 候选人状态失效时间
		elapsedTime := rand.Intn(150) + 150
		time.Sleep(time.Duration(elapsedTime) * time.Millisecond)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate {
			zlog.Debug("%d | %d | %2d | state: %s => follower, current election suspend",
				rf.me, rf.currentTerm, rf.leaderId, state2str[rf.state])
			// 变回follower，重置投票参数
			rf.state = Follower
			rf.voteFor = -1
		}
	}()

	// 向所有peer发送请求投票rpc
	for server := range rf.peers {
		// 排除自身
		if server == rf.me {
			continue
		}
		// 并发请求投票，成为leader的逻辑也在此处理
		server1 := server
		go func() {
			zlog.Debug("%d | %d | %2d | request vote %d", rf.me, rf.currentTerm, rf.leaderId, server1)
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(server1, args, reply); !ok {
				return
			}
			if !reply.VoteGranted {
				// 如果竞选的任期落后，则更新本节点的term，终止竞选
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm < reply.Term {
						zlog.Debug("%d | %d | %2d | state: %s => follower, higher term from %d",
							rf.me, rf.currentTerm, rf.leaderId, state2str[rf.state], server1)
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.leaderId = -1
						rf.voteFor = -1
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
			if rf.state == Candidate {
				// 本节点成为 leader
				zlog.Debug("%d | %d | %2d | state: %s => leader",
					rf.me, rf.currentTerm, rf.leaderId, state2str[rf.state])
				rf.state = Leader
				rf.leaderId = rf.me
				rf.voteFor = -1
				// 立刻发送心跳包通知其他节点,这里必须另开协程，否则会死锁
				go rf.heartbeat()
			}
		}()
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
		voteFor:     -1,
		logs:        make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}

	nap := LogEntry{
		Term: rf.currentTerm,
	}
	rf.logs = append(rf.logs, nap)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
