package zraft

import (
	"math/rand"
	"net/http"
	"net/rpc"
	"sync"
	"time"
	"zraft/zlog"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

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

const (
	intervalTime         = 20
	heartbeatTime        = 200
	electionTimeoutFrom  = 6000
	electionTimeoutRange = 2000
	leaderTimeout        = 20000
)

func GetRandomElapsedTime() int {
	return electionTimeoutFrom + rand.Intn(electionTimeoutRange)
}

func rpcDelay() {
	// if KConf.DelayRange > 0 {
	// 	ms := KConf.DelayFrom + rand.Intn(KConf.DelayRange)
	// 	time.Sleep(time.Duration(ms) * time.Millisecond)
	// }
}

type Peer struct {
	id         int
	addr       string
	rpcCli     *rpc.Client // rpc服务端的客户端
	connStatus int32
}

type Raft struct {
	mu         sync.Mutex // Lock to protect shared access to this peer's state
	peers      []*Peer    // RPC end points of all peers
	me         int        // this peer's index into peers[]
	addr       string     // rpc监听地址
	state      State
	applyCh    chan ApplyMsg
	leaderLost int32 // leader 是否超时

	// raft参数
	leaderId    int
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// draft
	reqCh   chan interface{}
	draftCh chan []interface{}
	drafts  map[string][]interface{}
	nApply  int32
}

func RunRaft(maddr, saddr string) {

	rf := &Raft{
		addr:        saddr,
		state:       Follower,
		leaderId:    -1,
		applyCh:     make(chan ApplyMsg),
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0}}, // 初始存一个默认entry，索引和任期为0
		commitIndex: 0,
		lastApplied: 0,
	}

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| make raft, peers.num:%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		len(rf.peers))

	// 开启rpc服务
	rf.rpcListen()

	// 注册节点信息，并获取其他节点信息
	rf.register(maddr)

	// 并行建立连接
	rf.connectPeers()

	time.Sleep(time.Second)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()

	go rf.test()

	select {}
}

func (rf *Raft) rpcListen() {
	// 放入协程防止阻塞后面函数
	go func() {
		rpc.Register(rf)
		rpc.HandleHTTP()
		err := http.ListenAndServe(rf.addr, nil)
		if err != nil {
			zlog.Error("http.ListenAndServe failed, err:%v", err)
		}
	}()
}

func (rf *Raft) register(maddr string) {
	time.Sleep(500 * time.Millisecond)

	// 连接master,注册节点信息，并获取其他节点信息
	zlog.Info("connect master ...")
	rpcCli, err := rpc.DialHTTP("tcp", maddr)
	if err != nil {
		zlog.Error("rpc.DialHTTP failed, err:%v", err)
	}

	args := &RegisterArgs{Addr: rf.addr}
	reply := &RegisterReply{}

	zlog.Info("register peer info ...")
	rpcCli.Call("Master.RegisterRpc", args, reply)

	// 重复addr注册或超过 PeerNum 限定节点数，注册失败
	if !reply.Ok {
		zlog.Error("register failed")
	}

	// 设置节点信息
	rf.peers = make([]*Peer, len(reply.Addrs))
	for i, addr := range reply.Addrs {
		if addr == rf.addr {
			rf.me = i
		}
		rf.peers[i] = &Peer{
			id:   i,
			addr: addr,
		}
	}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	zlog.Info("register success")
}

func (rf *Raft) connectPeers() {
	zlog.Info("build connect with other peers ...")
	// 等待多个连接任务结束再继续
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	for id, peer := range rf.peers {
		if id == rf.me {
			continue
		}
		p := peer
		go func() {
			// 每隔1s请求建立连接，60s未连接则报错
			t0 := time.Now()
			for time.Since(t0).Seconds() < 60 {
				rpcCli, err := rpc.DialHTTP("tcp", p.addr)
				if err == nil {
					zlog.Debug("connect (id=%d,addr=%s) success", p.id, p.addr)
					p.rpcCli = rpcCli
					wg.Done()
					return
				}
				zlog.Warn("connect (id=%d,addr=%s) error, err:%v", p.id, p.addr, err)
				time.Sleep(time.Second)
			}
			zlog.Error("connect (id=%d,addr=%s) failed, terminate", p.id, p.addr)
		}()
	}
	wg.Wait()
	zlog.Info("==== connect all peers success ====")
}
