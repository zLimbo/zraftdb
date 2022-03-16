package main

import (
	"log"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	node      *Node
	seqCh     chan int64
	logs      []*Log
	seq2cert  map[int64]*LogCert
	id2srvCli map[int64]*rpc.Client
	cliCli    *rpc.Client
	mu        sync.Mutex
	seqInc    int64
	view      int64
}

func (s *Server) assignSeq() int64 {
	// 后8位为节点id
	return atomic.AddInt64(&s.seqInc, 1e10)
}

func (s *Server) getCertOrNew(seq int64) *LogCert {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, ok := s.seq2cert[seq]
	if !ok {
		cert = &LogCert{
			seq:      seq,
			prepares: make(map[int64]*PrepareArgs),
			commits:  make(map[int64]*CommitArgs),
			prepareQ: make([]*PrepareArgs, 0),
			commitQ:  make([]*CommitArgs, 0),
		}
		s.seq2cert[seq] = cert
	}
	return cert
}

func (s *Server) RequestRpc(args *RequestArgs, reply *RequestReply) error {
	// 放入请求队列直接返回，后续异步通知客户端

	Debug("RequestRpc, from: %d", args.Req.ClientId)

	// 验证RequestMsg
	node := GetNode(args.Req.ClientId)
	digest := Sha256Digest(args.Req)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("RequestMsg verify error, from: %d", args.Req.ClientId)
		reply.Ok = false
		return nil
	}

	// leader 分配seq
	seq := s.assignSeq()
	s.getCertOrNew(seq).set(args, digest, s.view)
	s.seqCh <- seq

	// 返回信息
	reply.Seq = seq
	reply.Ok = true

	return nil
}

func (s *Server) PrePrepare(seq int64) {
	req, digest, view := s.getCertOrNew(seq).get()
	msg := &PrePrepareMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数
	args := &PrePrepareArgs{
		Msg:     msg,
		Sign:    sign,
		ReqArgs: req,
	}

	// 等待发完2f个节点再进入下一阶段
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var reply bool
			err := srvCli1.Call("Server.PrePrepareRpc", args, &reply)
			if err != nil {
				Error("Server.PrePrepareRpc %d error: %v", id1, err)
			}
		}()
	}
	Debug("PrePrepare %d ok", seq)
	s.Prepare(seq)
}

func (s *Server) PrePrepareRpc(args *PrePrepareArgs, reply *bool) error {
	msg := args.Msg
	Debug("PrePrepareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)
	// 预设返回失败
	*reply = false

	// 验证PrePrepareMsg
	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("PrePrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}

	// 验证RequestMsg
	reqArgs := args.ReqArgs
	node = GetNode(reqArgs.Req.ClientId)
	digest = Sha256Digest(reqArgs.Req)
	if !SliceEqual(digest, msg.Digest) {
		Warn("PrePrepareMsg error, req.digest != msg.Digest")
		return nil
	}
	ok = RsaVerifyWithSha256(digest, reqArgs.Sign, node.pubKey)
	if !ok {
		Warn("RequestMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}

	// 设置证明
	cert := s.getCertOrNew(msg.Seq)
	cert.set(reqArgs, digest, msg.View)

	// 进入Prepare投票
	go s.Prepare(cert.seq)

	// 计票
	s.verifyBallot(cert)

	// 返回成功
	*reply = true
	return nil
}

func (s *Server) Prepare(seq int64) {
	_, digest, view := s.getCertOrNew(seq).get()
	msg := &PrepareMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &PrepareArgs{
		Msg:  msg,
		Sign: sign,
	}
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var reply bool
			err := srvCli1.Call("Server.PrepareRpc", args, &reply)
			if err != nil {
				Error("Server.PrepareRpc %d error: %v", id1, err)
			}
		}()
	}
}

func (s *Server) PrepareRpc(args *PrepareArgs, reply *bool) error {
	msg := args.Msg
	Debug("PrepareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)

	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	cert := s.getCertOrNew(msg.Seq)
	cert.pushPrepare(args)
	s.verifyBallot(cert)

	*reply = true
	return nil
}

func (s *Server) Commit(seq int64) {
	// cmd := s.getCertOrNew(seq).getCmd() // req一定存在
	_, digest, view := s.getCertOrNew(seq).get()
	msg := &CommitMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &CommitArgs{
		Msg:  msg,
		Sign: sign,
	}
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var reply bool
			err := srvCli1.Call("Server.CommitRpc", args, &reply)
			if err != nil {
				Error("Server.CommitRpc %d error: %v", id1, err)
			}
		}()
	}
}

func (s *Server) CommitRpc(args *CommitArgs, reply *bool) error {
	msg := args.Msg
	Debug("CommitRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)

	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	cert := s.getCertOrNew(msg.Seq)
	cert.pushCommit(args)
	s.verifyBallot(cert)

	*reply = true
	return nil
}

func (s *Server) verifyBallot(cert *LogCert) {
	req, _, _ := cert.get()

	// cmd 为空则不进行后续阶段
	if req == nil {
		Debug("march, cmd is nil")
		return
	}
	// Debug("march, seq: %d, prepare: %d, commit: %d, stage: %d",
	// 	cert.seq, cert.prepareBallot(), cert.commitBallot(), cert.getStage())
	_, reqDigest, view := cert.get()

	switch cert.getStage() {
	case PrepareStage:
		argsQ := cert.popAllPrepares()
		for _, args := range argsQ {
			msg := args.Msg
			if cert.prepareVoted(msg.NodeId) { // 已投票
				continue
			}
			if view != msg.View {
				Warn("PrepareMsg error, view(%d) != msg.View(%d)", view, msg.View)
				continue
			}
			if !SliceEqual(reqDigest, msg.Digest) {
				Warn("PrePrepareMsg error, req.digest != msg.Digest")
				continue
			}
			// 验证PrepareMsg
			node := GetNode(msg.NodeId)
			digest := Sha256Digest(msg)
			ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
			if !ok {
				Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
				continue
			}
			cert.prepareVote(args)
		}
		// 2f + 1 (包括自身) 后进入 commit 阶段
		if cert.prepareBallot() >= 2*KConfig.FalultNum {
			cert.setStage(CommitStage)
			go s.Commit(cert.seq)
		}
		fallthrough // 进入后续判断
	case CommitStage:
		argsQ := cert.popAllCommits()
		for _, args := range argsQ {
			msg := args.Msg
			if cert.commitVoted(msg.NodeId) { // 已投票
				continue
			}
			if view != msg.View {
				Warn("PrepareMsg error, view(%d) != msg.View(%d)", view, msg.View)
				continue
			}
			if !SliceEqual(reqDigest, msg.Digest) {
				Warn("PrePrepareMsg error, req.digest != msg.Digest")
				continue
			}
			// 验证PrepareMsg
			node := GetNode(msg.NodeId)
			digest := Sha256Digest(msg)
			ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
			if !ok {
				Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
				continue
			}
			cert.commitVote(args)
		}
		// 2f + 1 (包括自身) 后进入 reply 阶段
		if cert.commitBallot() >= 2*KConfig.FalultNum {
			cert.setStage(ReplyStage)
			go s.Reply(cert.seq)
		}
	}
}

func (s *Server) Reply(seq int64) {
	Debug("Reply %d", seq)
	req, _, view := s.getCertOrNew(seq).get()
	msg := &ReplyMsg{
		View:      view,
		Seq:       seq,
		Timestamp: time.Now().UnixNano(),
		ClientId:  req.Req.ClientId,
		NodeId:    s.node.id,
		Result:    req.Req.Operator,
	}
	digest := Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	replyArgs := &ReplyArgs{
		Msg:  msg,
		Sign: sign,
	}
	var reply bool
	err := s.cliCli.Call("Client.ReplyRpc", replyArgs, &reply)
	if err != nil {
		Error("Client.ReplyRpc error: %v", err)
	}
}

func (s *Server) connect(coch chan<- interface{}) {
	ok := false
	for !ok {
		time.Sleep(time.Second) // 每隔一秒进行连接
		Info("build connect...")
		ok = true
		for id, node := range KConfig.Id2Node {
			if node == s.node {
				continue
			}
			if s.id2srvCli[id] == nil {
				cli, err := rpc.DialHTTP("tcp", node.addr)
				if err != nil {
					Warn("connect %s error: %v", node.addr, err)
					ok = false
				} else {
					s.id2srvCli[id] = cli
				}

			}
		}
		if s.cliCli == nil {
			cli, err := rpc.DialHTTP("tcp", KConfig.ClientNode.addr)
			if err != nil {
				Warn("connect %d error: %v", KConfig.ClientNode.addr, err)
				ok = false
			} else {
				s.cliCli = cli
			}
		}
	}
	Info("\n== connect success ==")
	coch <- 1
}

func (s *Server) workLoop(coch <-chan interface{}) {

	// 阻塞直到连接成功
	<-coch
	Info("start work loop")

	for seq := range s.seqCh {
		s.PrePrepare(seq)
	}
}

func RunServer(id int64) {
	server := &Server{
		node:      KConfig.Id2Node[id],
		seqCh:     make(chan int64, ChanSize),
		logs:      make([]*Log, 0),
		seq2cert:  make(map[int64]*LogCert),
		id2srvCli: make(map[int64]*rpc.Client),
	}
	// 每个分配序号后缀为节点id(8位)
	server.seqInc = server.node.id
	// 当前暂无view-change, view暂且设置为server id
	server.view = server.node.id

	coch := make(chan interface{})
	go server.connect(coch)
	go server.workLoop(coch)

	rpc.Register(server)
	rpc.HandleHTTP()
	if err := http.ListenAndServe(server.node.addr, nil); err != nil {
		log.Fatal("server error: ", err)
	}
}
