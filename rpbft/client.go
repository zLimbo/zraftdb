package main

import (
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Client struct {
	node       *Node
	applyCount int32
	start      time.Time
	id2srvCli  map[int64]*rpc.Client
	seq2cert   map[int64]*CmdCert
	sumLatency int64
	mu         sync.Mutex
	coch       chan interface{}
}

type ReplyArgs struct {
	NodeId int64
	Seq    int64
	Ok     bool
	Digest []byte
	Sign   []byte
}

func (c *Client) ReplyRpc(args *ReplyArgs, reply *bool) error {
	Debug("ReplyRpc, seq: %d, from: %d", args.Seq, args.NodeId)
	*reply = true

	// measure
	c.mu.Lock()
	defer c.mu.Unlock()

	cert := c.seq2cert[args.Seq]
	_, ok := cert.replys[args.NodeId]
	if ok {
		return nil
	}
	cert.replys[args.NodeId] = args.Sign
	replyCount := len(cert.replys)

	// Debug("replyCount: %d, f+1: %d", replyCount, KConfig.FalultNum+1)
	if replyCount != KConfig.FalultNum+1 {
		return nil
	}
	c.applyCount++
	take := ToSecond(time.Since(c.start))
	tps := float64(KConfig.BatchTxNum*int(c.applyCount)) / take
	latency := time.Since(cert.start)
	c.sumLatency += latency.Milliseconds()
	avgLatency := float64(c.sumLatency) / float64(c.applyCount)

	Info("apply: %d seq: %d take: %.2f tps: %.0f curLa: %d avgLa: %.2f",
		c.applyCount, args.Seq, take, tps, latency.Milliseconds(), avgLatency)

	c.coch <- args.Seq
	return nil
}

func (c *Client) connect(coch chan<- interface{}) {
	ok := false
	for !ok {
		time.Sleep(time.Second) // 每隔一秒进行连接
		ok = true
		for id, node := range KConfig.Id2Node {
			if c.id2srvCli[id] == nil {
				cli, err := rpc.DialHTTP("tcp", node.addr)
				if err != nil {
					Info("connect %d error: %v", node.addr, err)
					ok = false
				} else {
					c.id2srvCli[id] = cli
				}

			}
		}
	}
	Info("\n== connect success ==")
	coch <- 1
}

func (c *Client) sendReq(coch <-chan interface{}) {
	<-coch
	Info("start send req")
	c.start = time.Now()

	args := &RequestArgs{
		NodeId: c.node.id,
		Cmd: &Command{
			Seq:    0,
			FromId: c.node.id,
			Tx:     make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
		},
	}
	before := time.Now()
	args.Digest = Sha256Digest(args.Cmd)
	digestTake := time.Since(before)
	before = time.Now()
	args.Sign = RsaSignWithSha256(args.Digest, c.node.priKey)
	signTake := time.Since(before)
	Info("digest: %.2f (s), sign: %.2f (s)", ToSecond(digestTake), ToSecond(signTake))

	var reply bool
	for i := 0; i < KConfig.ReqNum; i++ {
		seq := c.node.id*1000 + int64(i)

		c.mu.Lock()
		c.seq2cert[seq] = &CmdCert{
			seq:    seq,
			start:  time.Now(),
			replys: make(map[int64][]byte),
		}
		c.mu.Unlock()

		args.Cmd.Seq = seq
		for id, srvCli := range c.id2srvCli {
			Debug("send req %d to node %d", seq, id)
			err := srvCli.Call("Server.RequestRpc", args, &reply)
			if err != nil {
				Error("Server.RequestRpc err: %v", err)
			}
			break
		}

		<-c.coch
	}
}

func runClient() {
	client := &Client{
		node:      KConfig.ClientNode,
		id2srvCli: make(map[int64]*rpc.Client),
		seq2cert:  make(map[int64]*CmdCert),
		coch:      make(chan interface{}, 100),
	}
	coch := make(chan interface{})
	go client.connect(coch)
	go client.sendReq(coch)

	rpc.Register(client)
	rpc.HandleHTTP()
	if err := http.ListenAndServe(client.node.addr, nil); err != nil {
		Error("client listen err: %v", err)
	}
}
