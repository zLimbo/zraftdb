package zraft

import (
	"bufio"
	"fmt"
	"os"
	"sync/atomic"
	"time"
	"zraft/zlog"
)

type RequestArgs struct {
	Cmd interface{}
}

type RequestReply struct {
	Index int
	Term  int
	Ok    bool
}

func (rf *Raft) RequestRpc(args *RequestArgs, reply *RequestReply) error {

	leaderId := func() int {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.me == rf.leaderId {
			reply.Index, reply.Term, reply.Ok = rf.start(args.Cmd)
			return -1
		}
		if rf.leaderId == -1 {
			reply.Ok = false
			return -1
		}
		return rf.leaderId
	}()

	if leaderId == -1 {
		return nil
	}

	return rf.peers[leaderId].rpcCli.Call("Raft.RequestRpc", args, reply)
}

func (rf *Raft) start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

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

		// rf.persist()
	}

	return index, term, isLeader
}

func (rf *Raft) getState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if atomic.LoadInt32(&rf.state) == Leader {
		isleader = true
	}

	return term, isleader
}

func (rf *Raft) persist(outCh chan []interface{}) {
	if !KConf.Persisted {
		for {
			<-outCh
		}
	}
	t0 := time.Now()
	path := fmt.Sprintf("%s/%02d_%s.log", KConf.LogDir, rf.me, t0.Format("2006-01-02_15:04:05"))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		zlog.Error("open log file fail, %v", err)
	}
	defer file.Close()
	write := bufio.NewWriter(file)
	for cmds := range outCh {
		for _, cmd := range cmds {
			write.WriteString(cmd.(string))
		}
	}
}

func (rf *Raft) stat(statCh chan interface{}) {
	t0 := time.Now()
	path := fmt.Sprintf("tps_%s.log", t0.Format("2006-01-02_15:04:05"))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		zlog.Error("open log file fail, %v", err)
	}
	defer file.Close()
	write := bufio.NewWriter(file)
	for st := range statCh {
		write.WriteString(st.(string))
	}
}

func (rf *Raft) applyLog() {

	outCh := make(chan []interface{}, 1000)
	go rf.persist(outCh)
	// statCh := make(chan interface{}, 1000)
	// go rf.stat(statCh)

	cmds := make([]interface{}, 0, KConf.EpochSize)
	t0 := time.Now()
	nApply := 0
	for msg := range rf.applyCh {
		nApply++
		cmds = append(cmds, msg.Command)
		if len(cmds) == cap(cmds) {
			tps := float64(KConf.EpochSize) / ToSecond(time.Since(t0))
			zlog.Info("%d|%2d|%d|%d|<%d,%d>| apply=%d, tps=%.2f",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				nApply, tps)
			outCh <- cmds
			// statCh <- strconv.Itoa(int(tps)) + " "
			cmds = make([]interface{}, 0, KConf.EpochSize)
			t0 = time.Now()
		}
	}
}
func (rf *Raft) test() {
	// reqTime := 5.0 // 请求时间

	zlog.Info("Start test")
	// return
	reqCount := 0
	format := fmt.Sprintf("%2d-%%-%dd\n", rf.me, KConf.ReqSize-4)

	k := MaxInt(10, 10*KConf.BatchSize/KConf.EpochSize)
	for {
		time.Sleep(intervalTime * time.Millisecond)
		if atomic.LoadInt32(&rf.state) == Leader {
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| test start, reqCount=%d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				reqCount)
		}

		for atomic.LoadInt32(&rf.state) == Leader {
			reqCount++
			rf.start(fmt.Sprintf(format, reqCount))
			for reqCount > (k*KConf.EpochSize)+rf.commitIndex {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}
