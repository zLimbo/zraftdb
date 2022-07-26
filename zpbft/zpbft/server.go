package zpbft

import (
	"fmt"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
	"zpbft/zkv"
	"zpbft/zlog"
)

type Peer struct {
	id     int32
	addr   string
	pubkey []byte
	rpcCli *rpc.Client // rpc服务端的客户端
}

type Server struct {
	mu         sync.Mutex         // 互斥量，用于并发编程中保护共享变量和临界区
	f          int                // 容错数，pbft节点数>=3f+1
	id         int32              // id由master分配，不和端口相关
	addr       string             // 服务器地址
	prikey     []byte             // 私钥签名
	pubkey     []byte             // 公钥验证
	maddr      string             // master地址
	peers      map[int32]*Peer    // 每个server需要知道其他server的信息
	leader     int32              // pbft的leader，默认为0，为第一个在master注册的节点
	view       int32              // 每一个leader有一个视图，换主后递增
	maxSeq     int64              // 每个请求都会分配唯一的seq，递增，即 seq = maxSeq++
	seqCh      chan int64         // 请求到达分配seq放入该通道，异步进行共识过程
	applySeqCh chan int64         // 共识完成后该请求就被提交，其中的命令可以执行在kv数据库上
	seq2cert   map[int64]*LogCert // 每个请求在共识过程中的验证信息
	zkv        *zkv.ZKV           // 一个简单的内存kv数据库
}

// 运行server
func RunServer(maddr, saddr, pri, pub string) {

	// 读取公私钥，pri，pub为路径，server参数必须提供
	prikey, pubkey := ReadKeyPair(pri, pub)
	s := &Server{
		addr:       saddr,
		prikey:     prikey,
		pubkey:     pubkey,
		maddr:      maddr,
		peers:      map[int32]*Peer{},
		seqCh:      make(chan int64, 100),
		applySeqCh: make(chan int64, 100),
		seq2cert:   make(map[int64]*LogCert),
		zkv:        zkv.NewZKV(),
	}

	// 开启rpc服务
	s.rpcListen()

	// 注册节点信息，并获取其他节点信息
	s.register()

	// 并行建立连接
	s.connectPeers()

	// 进行pbft服务
	s.pbft()
}

func (s *Server) rpcListen() {
	// 放入协程防止阻塞后面函数
	go func() {
		rpc.Register(s)
		rpc.HandleHTTP()
		err := http.ListenAndServe(s.addr, nil)
		if err != nil {
			zlog.Error("http.ListenAndServe failed, err:%v", err)
		}
	}()
}

func (s *Server) register() {
	time.Sleep(500 * time.Millisecond)

	// 连接master,注册节点信息，并获取其他节点信息
	zlog.Info("connect master ...")
	rpcCli, err := rpc.DialHTTP("tcp", s.maddr)
	if err != nil {
		zlog.Error("rpc.DialHTTP failed, err:%v", err)
	}

	args := &RegisterArgs{Addr: s.addr, Pubkey: s.pubkey}
	reply := &RegisterReply{}

	zlog.Info("register peer info ...")
	rpcCli.Call("Master.RegisterRpc", args, reply)

	// 重复addr注册或超过 PeerNum 限定节点数，注册失败
	if !reply.Ok {
		zlog.Error("register failed")
	}

	// 设置节点信息
	for i := range reply.Addrs {
		if reply.Addrs[i] == s.addr {
			s.id = int32(i)
			continue
		}
		s.peers[int32(i)] = &Peer{
			id:     int32(i),
			addr:   reply.Addrs[i],
			pubkey: reply.Pubkeys[i],
		}
	}

	// n = 3f + 1
	s.f = (len(reply.Addrs) - 1) / 3
	s.leader = 0
	zlog.Info("register success, id=%d, leader=%d", s.id, s.leader)
}

func (s *Server) connectPeers() {
	zlog.Info("build connect with other peers ...")
	// 等待多个连接任务结束再继续
	wg := sync.WaitGroup{}
	wg.Add(len(s.peers))
	for _, peer := range s.peers {
		p := peer
		go func() {
			// 每隔1s请求建立连接，10s未连接则报错
			t0 := time.Now()
			for time.Since(t0).Seconds() < 10 {
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

// 分配 seq，由leader分配
func (s *Server) assignSeq() int64 {
	// 后8位为节点id
	return atomic.AddInt64(&s.maxSeq, 1)
}

// 获取或创建的验证信息对象，存储该请求共识中的相关消息
func (s *Server) getCertOrNew(seq int64) *LogCert {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, ok := s.seq2cert[seq]
	if !ok {
		cert = &LogCert{
			seq:        seq,
			id2prepare: make(map[int32]*PrepareArgs),
			id2commit:  make(map[int32]*CommitArgs),
			prepareWQ:  make([]*PrepareArgs, 0),
			commitWQ:   make([]*CommitArgs, 0),
			stage:      PrepareStage,
		}
		s.seq2cert[seq] = cert
	}
	return cert
}

func (s *Server) RequestRpc(args *RequestArgs, reply *RequestReply) error {

	// 如果不是leader，将请求转发至leader
	if s.leader != s.id {
		return s.peers[s.leader].rpcCli.Call("Server.RequestRpc", args, reply)
	}

	// 客户端的请求暂不验证
	// leader 分配seq
	seq := s.assignSeq()
	s.getCertOrNew(seq).set(args, Digest(args.Req), s.view)

	// leader 放入请求队列直接返回，后续异步通知客户端
	// 使用协程：因为通道会阻塞
	go func() {
		s.seqCh <- seq
	}()

	// 返回信息
	reply.Seq = seq
	reply.Ok = true

	zlog.Info("I=%d L=%d seq=%d| stage=0:request <= %s, cmd: [%s]", s.id, s.leader, seq, args.Req.ClientAddr, args.Req.Command)
	return nil
}

func (s *Server) pbft() {
	zlog.Info("== start work loop ==")
	// 串行处理请求
	for reqSeq := range s.seqCh {
		s.prePrepare(reqSeq)
	}
}

func (s *Server) prePrepare(seq int64) {

	// 配置rpc参数
	req, digest, view := s.getCertOrNew(seq).get()
	msg := &PrePrepareMsg{View: view, Seq: seq, Digest: digest, PeerId: s.id}
	args := &PrePrepareArgs{Msg: msg, Sign: Sign(Digest(msg), s.prikey), ReqArgs: req}

	// pre-prepare广播
	ids := []int32{}
	count := int32(0)
	for _, peer := range s.peers {
		// 异步发送
		p := peer
		ids = append(ids, p.id)
		go func() {
			reply := &PrePrepareReply{}
			err := p.rpcCli.Call("Server.PrePrepareRpc", args, reply)
			if err != nil {
				zlog.Warn("Server.PrePrepareRpc %d error: %v", p.id, err)
			}
			// 等待发完2f个节点再进入下一阶段
			if atomic.AddInt32(&count, 1) == 2*int32(s.f) {
				s.prepare(seq)
			}
		}()
	}
	zlog.Info("I=%d L=%d seq=%d| stage=1:pre-prepare => %v", s.id, s.leader, seq, ids)
}

func (s *Server) PrePrepareRpc(args *PrePrepareArgs, reply *PrePrepareReply) error {
	msg := args.Msg
	zlog.Info("I=%d L=%d seq=%d| stage=1:pre-prepare <= %d", s.id, s.leader, msg.Seq, msg.PeerId)
	// 预设返回失败
	reply.Ok = false

	// 验证PrePrepareMsg
	peer := s.peers[args.Msg.PeerId]
	if !Verify(Digest(msg), args.Sign, peer.pubkey) {
		zlog.Warn("PrePrepareMsg verify error")
		return nil
	}

	// 验证RequestMsg
	if !SliceEqual(Digest(args.ReqArgs.Req), msg.Digest) {
		zlog.Warn("PrePrepareMsg error: req.digest != msg.Digest")
		return nil
	}

	// 设置证明
	cert := s.getCertOrNew(msg.Seq)
	cert.set(args.ReqArgs, msg.Digest, msg.View)

	// 进入Prepare投票
	go s.prepare(cert.seq)

	// 尝试计票
	go s.verifyBallot(cert)

	// 返回成功
	reply.Ok = true

	return nil
}

func (s *Server) prepare(seq int64) {

	// 配置rpc参数,相比PrePrepare无需req
	_, digest, view := s.getCertOrNew(seq).get()
	msg := &PrepareMsg{View: view, Seq: seq, Digest: digest, PeerId: s.id}
	args := &PrepareArgs{Msg: msg, Sign: Sign(Digest(msg), s.prikey)}

	// prepare广播
	ids := []int32{}
	for _, peer := range s.peers {
		// 异步发送
		p := peer
		ids = append(ids, p.id)
		go func() {
			reply := &PrepareReply{}
			if err := p.rpcCli.Call("Server.PrepareRpc", args, reply); err != nil {
				zlog.Warn("Server.PrePrepareRpc %d error: %v", p.id, err)
			}
		}()
	}
	zlog.Info("I=%d L=%d seq=%d| stage=2:prepare => %v", s.id, s.leader, seq, ids)
}

func (s *Server) PrepareRpc(args *PrepareArgs, reply *PrepareReply) error {
	msg := args.Msg
	zlog.Info("I=%d L=%d seq=%d| stage=2:prepare <= %d", s.id, s.leader, msg.Seq, msg.PeerId)

	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	cert := s.getCertOrNew(msg.Seq)
	cert.pushPrepare(args)

	// 尝试计票
	go s.verifyBallot(cert)

	reply.Ok = true
	return nil
}

func (s *Server) commit(seq int64) {
	// TODO 请给出你的实现 (可参考prepare代码)
}

func (s *Server) CommitRpc(args *CommitArgs, reply *CommitReply) error {
	// TODO 请给出你的实现 (可参考PrepareRpc代码)
	return nil
}

func (s *Server) verifyBallot(cert *LogCert) {
	// req 为空则不进行后续阶段
	if cert.req == nil {
		zlog.Debug("I=%d L=%d seq=%d| verify ballot stop, req is nil", s.id, s.leader, cert.seq)
		return
	}
	if cert.getStage() == PrepareStage {
		s.verifyBallotPrepare(cert)
	}
	if cert.getStage() == CommitStage {
		s.verifyBallotCommit(cert)
	}
}

func (s *Server) verifyBallotPrepare(cert *LogCert) {
	s.mu.Lock()
	defer s.mu.Unlock()
	argsQ := cert.popAllPrepares()
	for _, args := range argsQ {
		msg := args.Msg
		if cert.prepareVoted(msg.PeerId) {
			// 该peer重复投票，不计算
			continue
		}
		// 验证view是否是当前view
		if cert.view != msg.View {
			zlog.Warn("verify ballot, PrepareMsg error, view(%d) != msg.View(%d)", cert.view, msg.View)
			continue
		}
		// 验证摘要是否相同
		if !SliceEqual(cert.digest, msg.Digest) {
			zlog.Warn("verify ballot, PrePrepareMsg error, req.digest != msg.Digest")
			continue
		}
		// 验证PrepareMsg签名信息
		peer := s.peers[args.Msg.PeerId]
		digest := Digest(msg)
		ok := Verify(digest, args.Sign, peer.pubkey)
		if !ok {
			zlog.Warn("verify ballot, PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
			continue
		}
		// 将该消息放入验证集中
		cert.prepareVote(args)
	}
	zlog.Debug("I=%d L=%d seq=%d| verify ballot, stage=%d prepare=%d commit=%d",
		s.id, s.leader, cert.seq, cert.getStage(), cert.prepareBallot(), cert.commitBallot())
	// 2f + 1 (包括自身) 后进入 commit 阶段
	if cert.getStage() == PrepareStage && cert.prepareBallot() >= 2*s.f {
		cert.setStage(CommitStage)
		go s.commit(cert.seq)
		return
	}
}

func (s *Server) verifyBallotCommit(cert *LogCert) {
	// TODO 请给出你的实现 (可参考verifyBallotPrepare代码)
}

// apply 日志，执行完后返回
func (s *Server) apply(seq int64) {
	cmd := s.getCertOrNew(seq).req.Req.Command
	result := s.zkv.Execute(cmd)
	go s.reply(seq, result)
	zlog.Info("I=%d L=%d seq=%d| execute cmd:[%s], result:[%s]", s.id, s.leader, seq, cmd, result)

	// 打印 zkv 信息
	time.Sleep(100 * time.Millisecond)
	all := s.zkv.All()
	fmt.Println("\n==== zkv ====")
	fmt.Printf("%s", all)
	fmt.Println("=============")
	fmt.Println()
}

func (s *Server) reply(seq int64, result string) {

	req, _, view := s.getCertOrNew(seq).get()
	msg := &ReplyMsg{
		View:       view,
		Seq:        seq,
		Timestamp:  time.Now().UnixNano(),
		ClientAddr: req.Req.ClientAddr,
		PeerId:     s.id,
		Result:     result,
	}
	replyArgs := &ReplyArgs{Msg: msg, Sign: Sign(Digest(msg), s.prikey)}
	reply := &ReplyReply{}

	zlog.Info("I=%d L=%d seq=%d| stage=4:reply => %s", s.id, s.leader, seq, req.Req.ClientAddr)
	rpcCli, err := rpc.DialHTTP("tcp", req.Req.ClientAddr)
	if err != nil {
		zlog.Warn("dial client %s failed", req.Req.ClientAddr)
		return
	}
	if err := rpcCli.Call("Client.ReplyRpc", replyArgs, reply); err != nil {
		zlog.Warn("rpcCli.Call(\"Client.ReplyRpc\") failed, %v", err)
	}
}
