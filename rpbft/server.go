package main

import (
	"log"
	"net/http"
	"net/rpc"
	"sync"
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
}

type RequestArgs struct {
	NodeId int64
	Cmd    *Command
	Digest []byte
	Sign   []byte
}

func (s *Server) getCertOrNew(seq int64) *LogCert {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, ok := s.seq2cert[seq]
	if !ok {
		cert = &LogCert{
			prepares: make(map[int64][]byte),
			commits:  make(map[int64][]byte),
		}
		s.seq2cert[seq] = cert
	}
	return cert
}

func (s *Server) RequestRpc(args *RequestArgs, reply *bool) error {
	Debug("RequestRpc, seq: %d, from: %d", args.Cmd.Seq, args.NodeId)
	// 放入请求队列直接返回，后续异步通知客户端

	cmd := args.Cmd
	s.getCertOrNew(cmd.Seq).setCmd(cmd)
	s.seqCh <- cmd.Seq
	*reply = true

	return nil
}

func (s *Server) ballot(cert *LogCert) {
	cmd := cert.getCmd()

	// cmd 为空则不进行后续阶段
	if cmd == nil {
		Debug("march, cmd is nil")
		return
	}
	Debug("march, seq: %d, prepare: %d, commit: %d, stage: %d",
		cert.cmd.Seq, cert.prepareBallot(), cert.commitBallot(), cert.getStage())

	switch cert.getStage() {
	case PrepareStage:
		// 2f + 1 (包括自身) 后进入 commit 阶段
		if cert.prepareBallot() >= 2*KConfig.FalultNum {
			cert.setStage(CommitStage)
			go s.Commit(cmd.Seq)
		}
		fallthrough // 进入后续判断
	case CommitStage:
		// 2f + 1 (包括自身) 后进入 reply 阶段
		if cert.commitBallot() >= 2*KConfig.FalultNum {
			cert.setStage(ReplyStage)
			go s.Reply(cmd.Seq)
		}
	}
}

type PrePrepareArgs struct {
	NodeId int64
	Cmd    *Command
	Digest []byte
	Sign   []byte
}

func (s *Server) PrePrepareRpc(args *PrePrepareArgs, reply *bool) error {
	Debug("PrePrepareRpc, seq: %d, from: %d", args.Cmd.Seq, args.NodeId)
	*reply = true

	cmd := args.Cmd
	cert := s.getCertOrNew(cmd.Seq)
	cert.setCmd(cmd)

	go s.Prepare(cmd.Seq)
	s.ballot(cert)

	return nil
}

func (s *Server) PrePrepare(seq int64) {
	cmd := s.getCertOrNew(seq).getCmd()

	args := &PrePrepareArgs{
		NodeId: s.node.id,
		Cmd:    cmd,
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
	Debug("PrePrepare %d ok", cmd.Seq)
	s.Prepare(cmd.Seq)
}

type PrepareArgs struct {
	NodeId int64
	Seq    int64
	Digest []byte
	Sign   []byte
}

func (s *Server) PrepareRpc(args *PrepareArgs, reply *bool) error {
	Debug("PrepareRpc, seq: %d, from: %d", args.Seq, args.NodeId)

	*reply = true
	cert := s.getCertOrNew(args.Seq)
	cert.prepareVote(args)
	s.ballot(cert)

	return nil
}

func (s *Server) Prepare(seq int64) {
	cmd := s.getCertOrNew(seq).getCmd() // req一定存在

	args := &PrepareArgs{
		NodeId: s.node.id,
		Seq:    cmd.Seq,
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

type CommitArgs struct {
	NodeId int64
	Seq    int64
	Digest []byte
	Sign   []byte
}

func (s *Server) CommitRpc(args *CommitArgs, reply *bool) error {
	Debug("CommitRpc, seq: %d, from: %d", args.Seq, args.NodeId)

	*reply = true
	cert := s.getCertOrNew(args.Seq)
	cert.commitVote(args)
	s.ballot(cert)

	return nil
}

func (s *Server) Commit(seq int64) {
	cmd := s.getCertOrNew(seq).getCmd() // req一定存在

	args := &CommitArgs{
		NodeId: s.node.id,
		Seq:    cmd.Seq,
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

func (s *Server) Reply(seq int64) {
	Debug("Reply %d", seq)
	replyArgs := &ReplyArgs{
		NodeId: s.node.id,
		Seq:    seq,
		Ok:     true,
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
					Info("connect %d error: %v", node.addr, err)
					ok = false
				} else {
					s.id2srvCli[id] = cli
				}

			}
		}
		if s.cliCli == nil {
			cli, err := rpc.DialHTTP("tcp", KConfig.ClientNode.addr)
			if err != nil {
				Info("connect %d error: %v", KConfig.ClientNode.addr, err)
				ok = false
			} else {
				s.cliCli = cli
			}
		}
	}
	Info("== connect success ==")
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

func runServer(id int64) {
	server := &Server{
		node:      KConfig.Id2Node[id],
		seqCh:     make(chan int64, ChanSize),
		logs:      make([]*Log, 0),
		seq2cert:  make(map[int64]*LogCert),
		id2srvCli: make(map[int64]*rpc.Client),
	}
	coch := make(chan interface{})
	go server.connect(coch)
	go server.workLoop(coch)

	rpc.Register(server)
	rpc.HandleHTTP()
	if err := http.ListenAndServe(server.node.addr, nil); err != nil {
		log.Fatal("server error: ", err)
	}
}
