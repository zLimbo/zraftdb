package zraft

import (
	"sync/atomic"
	"time"
	"zraft/zlog"
)

func (rf *Raft) ticker() {
	for {
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
	ballot := 1
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
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| request vote %d, start",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server1)

			reply := &RequestVoteReply{}
			before := time.Now()
			ok := rf.sendRequestVote(server1, args, reply)
			take := time.Since(before)
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| request vote %d, ok=%v, take=%v",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server1, ok, take)
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
	if args.Term != rf.currentTerm {
		zlog.Trace("%d|%2d|%d|%d|<%d,%d>| request vote %d, ok but expire, args.Term=%d, rf.currentTerm=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, args.Term, rf.currentTerm)
		return
	}

	if !reply.VoteGranted {
		// 如果竞选的任期落后，则更新本节点的term，终止竞选
		if rf.currentTerm < reply.Term {
			zlog.Info("%d|%2d|%d|%d|<%d,%d>| state:%s=>follower, higher term from %d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				state2str[atomic.LoadInt32(&rf.state)], server)
			atomic.StoreInt32(&rf.state, Follower)
			rf.currentTerm = reply.Term
			rf.leaderId = -1
			rf.votedFor = -1
		}
		return
	}
	// 增加票数
	*ballot++
	zlog.Trace("%d|%2d|%d|%d|<%d,%d>| request vote %d, peer.num=%d, current ballot=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server, len(rf.peers), *ballot)
	// 须获得半数以上的投票才能成为leader
	if *ballot*2 <= len(rf.peers) {
		return
	}
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
		go rf.timingHeartbeatForAll()
		go rf.appendEntriesForAll()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rpcDelay()
	if err := rf.peers[server].rpcCli.Call("Raft.RequestVoteRpc", args, reply); err != nil {
		zlog.Warn("%v", err)
		return false
	}
	rpcDelay()
	return true
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVoteRpc(args *RequestVoteArgs, reply *RequestVoteReply) error {

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
		atomic.StoreInt32(&rf.state, Follower) // 如果是leader或candidate重新变回followe
		// TODO 请求投票如果有更大任期则不能重置超时！！！
		// atomic.StoreInt32(&rf.leaderLost, 0)   // 重置超时flag
	}

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLogTerm < args.LastLogTerm ||
			lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| vote for %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| reject vote for %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.CandidateId)
	}

	return nil
}
