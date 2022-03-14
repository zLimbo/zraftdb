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
	seq2cert  map[int64]*MsgCert
	seq2index map[int64]int // seq -> logIndex
	id2srvCli map[int64]*rpc.Client
	cliCli    *rpc.Client
	mu        sync.Mutex
}

type RequestArgs struct {
	Req    *Request
	Digest []byte
	Sign   []byte
}

func (s *Server) getCert(seq int64) (*MsgCert, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, ok := s.seq2cert[seq]
	return cert, ok
}

func (s *Server) setCert(req *Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq2cert[req.Seq] = &MsgCert{
		req: req,
	}
}

func (s *Server) RequestRpc(args *RequestArgs, reply *bool) error {

	// 放入请求队列直接返回，后续异步通知客户端

	s.seq2cert[args.Req.Seq] = &MsgCert{
		req: args.Req,
	}
	s.seqCh <- args.Req.Seq
	*reply = true

	return nil
}

type PrePrepareArgs struct {
	Req    *Request
	Digest []byte
	Sign   []byte
}

func (s *Server) PrePrepareRpc(args *PrePrepareArgs, reply *bool) error {
	Debug("PrePrepare %d", args.Req.Seq)
	*reply = true

	s.setCert(args.Req)

	go s.Prepare(args.Req.Seq)
	return nil
}

func (s *Server) PrePrepare(seq int64) {
	cert, _ := s.getCert(seq)
	req := cert.req
	// 等待发完2f个节点再进入下一阶段
	// var wg sync.WaitGroup
	for id, srvCli := range s.id2srvCli {
		// wg.Add(1)
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			// defer wg.Done()
			args := &PrePrepareArgs{
				Req: req,
			}
			var reply bool
			err := srvCli1.Call("Server.PrePrepareRpc", args, &reply)
			if err != nil {
				Error("Server.PrePrepareRpc %d error: %v", id1, err)
			}
		}()
	}
	Debug("PrePrepare %d ok", req.Seq)
	// wg.Wait()
	s.Prepare(req.Seq)
}

type PrepareArgs struct {
	Seq    int64
	Digest []byte
	Sign   []byte
}

func (s *Server) PrepareRpc(args *PrepareArgs, reply *bool) error {
	Debug("PrepareRpc %d", args.Seq)

	cert, ok := s.getCert(args.Seq)
	if !ok {
		*reply = false
		return nil
	}
	*reply = true

	// 增加投票数
	atomic.AddInt32(&cert.prepareBallot, 1)
	ballot := atomic.LoadInt32(&cert.prepareBallot)
	stage := atomic.LoadInt32(&cert.stage)

	// 2f + 1 (包括自身) 后进入 commit 阶段
	if int(ballot) >= 2*KConfig.FalultNum && stage < CommitStage {
		atomic.StoreInt32(&cert.stage, CommitStage)
		go s.Commit(args.Seq)
	}

	return nil
}

func (s *Server) Prepare(seq int64) {
	var req *Request
	repeat := 0
	for {
		cert, ok := s.getCert(seq)
		if ok {
			req = cert.req
			break
		}
		repeat++
		if repeat == 10 {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}

	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			args := &PrepareArgs{
				Seq: req.Seq,
			}
			var reply bool
			for {
				err := srvCli1.Call("Server.PrepareRpc", args, &reply)
				if err != nil {
					Error("Server.PrepareRpc %d error: %v", id1, err)
				}
				if reply {
					break
				}
				// reply为false说明该节点无req
				args1 := &PrePrepareArgs{
					Req: req,
				}
				err = srvCli1.Call("Server.PrePrepareRpc", args1, &reply)
				if err != nil {
					Error("Server.PrepareRpc %d error: %v", id1, err)
				}
			}
		}()
	}
}

type CommitArgs struct {
	Seq    int64
	Digest []byte
	Sign   []byte
}

func (s *Server) CommitRpc(args *PrepareArgs, reply *bool) error {
	Debug("CommitRpc %d", args.Seq)
	cert, ok := s.getCert(args.Seq)
	if !ok {
		*reply = false
		return nil
	}
	*reply = true

	// 增加投票数
	atomic.AddInt32(&cert.commitBallot, 1)
	ballot := atomic.LoadInt32(&cert.commitBallot)
	stage := atomic.LoadInt32(&cert.stage)

	// 2f + 1 (包括自身) 后进入 reply 阶段
	if int(ballot) >= 2*KConfig.FalultNum && stage < ReplyStage {
		atomic.StoreInt32(&cert.stage, ReplyStage)
		go s.Reply(args.Seq)
	}

	return nil
}

func (s *Server) Commit(seq int64) {
	cert, _ := s.getCert(seq)
	req := cert.req

	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			args := &CommitArgs{
				Seq: req.Seq,
			}
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
		Seq: seq,
		Ok:  true,
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
		// s.seq2Req[req.Seq] = req
		// s.pbft(PrePrepareStage, req.Seq)
	}
}

func runServer(id int64) {
	server := &Server{
		node:      KConfig.Id2Node[id],
		seqCh:     make(chan int64, ChanSize),
		logs:      make([]*Log, 0),
		seq2cert:  make(map[int64]*MsgCert),
		seq2index: make(map[int64]int),
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
