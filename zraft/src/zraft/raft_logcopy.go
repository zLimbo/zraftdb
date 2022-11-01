package zraft

import (
	"sort"
	"sync/atomic"
	"time"
	"zraft/zlog"
)

// 发送心跳
func (rf *Raft) timingHeartbeatForAll() {

	zlog.Trace("%d|%2d|%d|%d|<%d,%d>| timingHeartbeatForAll",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term)

	half := len(rf.peers) / 2
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		atomic.StoreInt32(&rf.peers[server].connStatus, 0)
	}

	for atomic.LoadInt32(&rf.state) == Leader {
		peerConnBmap := make(map[int]struct{})
		for t := 0; t < leaderTimeout; t += heartbeatTime {
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				// 如果heartbeatTime时间内未发送rpc，则发送心跳
				if atomic.LoadInt32(&rf.peers[server].connStatus) == 0 {
					go rf.heartbeatForOne(server)
				} else {
					atomic.StoreInt32(&rf.peers[server].connStatus, 0)
					peerConnBmap[server] = struct{}{}
				}
			}
			// 定时发送心跳
			time.Sleep(heartbeatTime * time.Millisecond)
		}

		// 每 leaderTimeout 时间统计连接状态，如果半数未连接则退出leader状态
		connectCount := len(peerConnBmap)
		if connectCount >= half {
			continue
		}

		// rf.mu.Lock()
		// defer rf.mu.Unlock()
		// // 如果连接数少于半数，则退化为 follower
		// zlog.Info("%d|%2d|%d|%d|<%d,%d>| leader timeout, connect count=%d(<%d), state:%s=>follower",
		// 	rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		// 	connectCount, half, state2str[atomic.LoadInt32(&rf.state)])
		// atomic.StoreInt32(&rf.state, Follower)
		// rf.leaderId = -1
		return
	}
}

func (rf *Raft) heartbeatForOne(server int) {
	args, stop := func() (*AppendEntriesArgs, bool) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if atomic.LoadInt32(&rf.state) != Leader {
			return nil, true
		}
		return &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
		}, false
	}()

	if stop {
		return
	}

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d, start",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server)

	reply := &AppendEntriesReply{}
	before := time.Now()
	ok := rf.sendAppendEntries(server, args, reply)
	take := time.Since(before)

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d, ok=%v, take=%v",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server, ok, take)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if atomic.LoadInt32(&rf.state) != Leader {
		return
	}

	// 超时后过期的回复消息
	if args.Term < rf.currentTerm {
		zlog.Trace("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d, ok but expire, args.Term=%d, rf.currentTerm=%d, take=%v",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, args.Term, rf.currentTerm, take)
		return
	}

	// 保持连接置位
	atomic.StoreInt32(&rf.peers[server].connStatus, 1)

	if rf.currentTerm < reply.Term {
		zlog.Info("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d, higher term, state:%s=>follower",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, state2str[atomic.LoadInt32(&rf.state)])
		rf.currentTerm = reply.Term
		atomic.StoreInt32(&rf.state, Follower)
		rf.leaderId = -1
		return
	}
}

func (rf *Raft) appendEntriesForAll() {
	zlog.Trace("%d|%2d|%d|%d|<%d,%d>| appendEntriesForAll",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.timingAppendEntriesForOne(server)
	}
}

func (rf *Raft) timingAppendEntriesForOne(server int) {
	zlog.Trace("%d|%2d|%d|%d|<%d,%d>| timingAppendEntriesForOne, server=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server)
	logMatched := false
	nEntriesCopy := 0
	commitIndex := 0

	// TODO: 串行发送rpc，并行会遇到state不是leader却依然发送和处理信息的情况, 也存在rpc幂等性问题
	for atomic.LoadInt32(&rf.state) == Leader {

		// 前一个rpc发送成功，立即判断是否要发下一个
		for atomic.LoadInt32(&rf.state) == Leader {
			// 检查是否已不是leader或被kill
			needSend := func() bool {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 有新的日志需要同步或有更新的commitIndex
				return len(rf.log)-1 >= rf.nextIndex[server] || commitIndex != rf.commitIndex
			}()
			if needSend {
				break
			}
			time.Sleep(intervalTime * time.Millisecond)
		}

		// go rf.heartbeatForOne(server)

		args, stop := func() (*AppendEntriesArgs, bool) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if atomic.LoadInt32(&rf.state) != Leader {
				return nil, true
			}

			startIndex := rf.nextIndex[server]
			if logMatched {
				// 指数递增拷贝
				// nEntriesCopy = MinInt(nEntriesCopy*nEntriesCopy+1, len(rf.log)-startIndex)
				// 全部取出拷贝
				if KConf.Draft {
					nEntriesCopy = MinInt(1, len(rf.log)-startIndex)
				} else {
					nEntriesCopy = MinInt(len(rf.log)-startIndex, KConf.BatchSize)
				}
			} else {
				nEntriesCopy = MinInt(1, len(rf.log)-startIndex)
			}

			return &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: startIndex - 1,
				PrevLogTerm:  rf.log[startIndex-1].Term,
				Entries:      rf.log[startIndex : startIndex+nEntriesCopy],
				LeaderCommit: rf.commitIndex,
			}, false
		}()

		if stop {
			return
		}

		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, entries=<%d, %d], PrevLogIndex=%d, PrevLogTerm=%d, LeaderCommit=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

		reply := &AppendEntriesReply{}
		before := time.Now()
		ok := rf.sendAppendEntries(server, args, reply)
		take := time.Since(before)

		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, ok=%v, take=%v",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, ok, take)

		// 发送失败立刻重发
		if !ok {
			// logMatched = false
			continue
		}

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if atomic.LoadInt32(&rf.state) != Leader {
				return
			}

			// 超时后过期的回复消息
			if args.Term < rf.currentTerm {
				zlog.Trace("%d|%2d|%d|%d|<%d,%d>| append entries to %d, ok but expire, args.Term=%d, rf.currentTerm=%d, take=%v",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server, args.Term, rf.currentTerm, take)
				return
			}

			// 保持连接置位
			atomic.StoreInt32(&rf.peers[server].connStatus, 1)

			// 遇到更高任期，成为follower
			if rf.currentTerm < reply.Term {
				zlog.Info("%d|%2d|%d|%d|<%d,%d>| append entries to %d, higher term, state:%s=>follower",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server, state2str[atomic.LoadInt32(&rf.state)])
				rf.currentTerm = reply.Term
				atomic.StoreInt32(&rf.state, Follower)
				rf.leaderId = -1
				return
			}

			// 如果不成功，说明PrevLogIndex不匹配
			if !reply.Success {

				zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, no match, PrevLogIndex=%d, PrevLogTerm=%d, reply.LogIndex=%d, reply.term=%d",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server, args.PrevLogIndex, args.PrevLogTerm, reply.LogIndex, reply.Term)

				logMatched = rf.matchNextIndex(server, args.PrevLogIndex, args.PrevLogTerm, reply.LogIndex, reply.Term)
				return
			}

			if len(args.Entries) != 0 {
				// 复制到副本成功
				rf.matchIndex[server] = MaxInt(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}

			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, match ok, copy entries=<%d, %d], match.index=%d, next.index=%d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), rf.matchIndex[server], rf.nextIndex[server])

			// 本server commitIndex的值
			commitIndex = MinInt(args.LeaderCommit, rf.matchIndex[server])

			// 推进leader的commit
			rf.advanceLeaderCommit()
			// follower和leader日志匹配成功，可以发送后续日志
			logMatched = true
		}()
	}
}

func (rf *Raft) matchNextIndex(server, prevLogIndex, prevLogTerm, replyLogIndex, replyLogTerm int) bool {

	if rf.log[replyLogIndex].Term == replyLogTerm {
		// 刚好匹配，从下一个开始
		rf.nextIndex[server] = replyLogIndex + 1
		return true
	} else if rf.log[replyLogIndex].Term < replyLogTerm {
		// 从前面试起
		rf.nextIndex[server] = replyLogIndex
		return false
	}
	// 二分优化快速查找
	left, right := rf.matchIndex[server], replyLogIndex
	for left < right {
		mid := left + (right-left)/2
		zlog.Trace("%d|%2d|%d|%d|<%d,%d>| append entries to %d, for match, rf.log[%d].Term=%d, reply.Term=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, mid, rf.log[mid].Term, replyLogTerm)
		if rf.log[mid].Term <= replyLogTerm {
			left = mid + 1
		} else {
			right = mid
		}
	}
	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, for match, rf.log[%d].Term=%d, reply.Term=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server, left, rf.log[left].Term, replyLogTerm)

	// 最近位置索引大一 (left - 1) + 1
	rf.nextIndex[server] = left
	return false
}

func (rf *Raft) advanceLeaderCommit() {
	// 判断是否需要递增 commitIndex，排序找出各个匹配的中位值就是半数以上都接受的日志
	indexes := make([]int, 0, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			indexes = append(indexes, rf.matchIndex[i])
		}
	}
	sort.Ints(indexes)
	newCommitIndex := indexes[len(indexes)-len(rf.peers)/2]
	// 相同任期才允许apply，避免被commit日志被覆盖的情况
	if rf.log[newCommitIndex].Term == rf.currentTerm && newCommitIndex > rf.commitIndex {
		// apply
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| step commit: %d => %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			rf.commitIndex, newCommitIndex)

		rf.applyLogEntries(rf.commitIndex+1, newCommitIndex)
		rf.commitIndex = newCommitIndex
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rpcDelay()
	if err := rf.peers[server].rpcCli.Call("Raft.AppendEntriesRpc", args, reply); err != nil {
		zlog.Error("%v", err)
		return false
	}
	rpcDelay()
	return true
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term     int  // currentTerm, for leader to update itself
	LogIndex int  // 换主后，为leader快速定位匹配日志
	Success  bool // success true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntriesRpc(args *AppendEntriesArgs, reply *AppendEntriesReply) error {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	atomic.StoreInt32(&rf.state, Follower) // 无论原来状态是什么，状态更新为follower
	atomic.StoreInt32(&rf.leaderLost, 0)   // 重置超时flag
	rf.currentTerm = args.Term             // 新的Term应该更高

	// 新的leader产生
	if args.LeaderId != rf.leaderId {
		zlog.Info("%d|%2d|%d|%d|<%d,%d>| state:%s=>follower, new leader %d[addr = %s]",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			state2str[atomic.LoadInt32(&rf.state)], args.LeaderId, rf.peers[args.LeaderId].addr)
		rf.leaderId = args.LeaderId
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	// leader 的心跳消息，直接返回
	if args.PrevLogIndex < 0 {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| heartbeat from %d, reply.Term=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.LeaderId, reply.Term)
		reply.LogIndex = -100
		return nil
	}

	// 相同日志未直接定位到，需多轮交互
	if !rf.foundSameLog(args, reply) {
		return nil
	}

	// 对日志进行操作
	rf.updateEntries(args, reply)
	return nil
}

func (rf *Raft) foundSameLog(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(rf.log) <= args.PrevLogIndex {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, no found same log, len(rf.log)=%d <= args.PrevLogIndex=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.LeaderId, len(rf.log), args.PrevLogIndex)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, no found same log, rf.log[%d].Term=%d != args.PrevLogTerm=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
	} else {
		return true
	}

	index := MinInt(args.PrevLogIndex, len(rf.log)-1)
	if rf.log[index].Term > args.PrevLogTerm {
		// 如果term不等，则采用二分查找找到最近匹配的日志索引
		left, right := rf.commitIndex, index
		for left < right {
			mid := left + (right-left)/2
			zlog.Trace("%d|%2d|%d|%d|<%d,%d>| from %d, no found same log, rf.log[%d].Term=%d, args.PrevLogTerm=%d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				args.LeaderId, mid, rf.log[mid].Term, args.PrevLogTerm)
			if rf.log[mid].Term <= args.PrevLogTerm {
				left = mid + 1
			} else {
				right = mid
			}
		}
		index = left - 1
	}

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, no found same log, rf.log[%d].Term=%d, args.PrevLogTerm=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		args.LeaderId, index, rf.log[index].Term, args.PrevLogTerm)

	reply.Term = rf.log[index].Term
	reply.LogIndex = index
	reply.Success = false
	return false
}

func (rf *Raft) updateEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 截断后面的日志
	if len(rf.log) > args.PrevLogIndex+1 {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, truncate log, rf.len: %d => %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.LeaderId, len(rf.log), args.PrevLogIndex+1)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	// 添加日志
	rf.log = append(rf.log, args.Entries...)

	// 在log变更时进行持久化
	// rf.persist()

	// 推进commit和apply
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = MinInt(args.LeaderCommit, len(rf.log)-1)
	rf.applyLogEntries(oldCommitIndex+1, rf.commitIndex)
	rf.lastApplied = rf.commitIndex

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, append %d entries: <%d, %d], commit: <%d, %d]",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		args.LeaderId, len(args.Entries), args.PrevLogIndex, len(rf.log)-1, oldCommitIndex, rf.commitIndex)
}

func (rf *Raft) applyLogEntries(left, right int) {
	for i := left; i <= right; i++ {
		zlog.Trace("%d|%2d|%d|%d|<%d,%d>| from %d, apply, commandIndex=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			i, rf.commitIndex)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
	}
}
