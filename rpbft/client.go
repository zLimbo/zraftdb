package main

import (
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Client struct {
	node        *Node
	commitCount int32
	start       time.Time
	id2srvCli   map[int64]*rpc.Client
	seq2cert    map[int64]*ReqCert
	sumLatency  int64
	mu          sync.Mutex
	coch        chan interface{}
}

type ReplyArgs struct {
	Seq int64
	Ok  bool
}

func (c *Client) ReplyRpc(args *ReplyArgs, reply *bool) error {
	Info("Reply %d", args.Seq)
	*reply = true

	c.mu.Lock()
	defer c.mu.Unlock()

	cert := c.seq2cert[args.Seq]
	cert.replyCount++

	if cert.replyCount != KConfig.FalultNum+1 {
		return nil
	}
	c.commitCount++
	take := ToSecond(time.Since(c.start))
	tps := float64(KConfig.BatchTxNum*int(c.commitCount)) / take
	latency := time.Since(cert.start)
	c.sumLatency += latency.Milliseconds()
	avgLatency := float64(c.sumLatency) / float64(c.commitCount)

	Info("commit: %d, take: %.2f (s), tps: %.2f, cur_latency: %d (ms), avg_latency: %.2f",
		c.commitCount, take, tps, latency.Milliseconds(), avgLatency)
	c.coch <- args.Seq
	return nil
}

func (c *Client) connect(coch chan<- interface{}) {
	ok := false
	for !ok {
		time.Sleep(time.Second) // 每隔一秒进行连接
		ok = true
		for id, node := range KConfig.Id2Node {
			// if node == c.node {
			// 	continue
			// }
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
		Req: &Request{
			Seq:      0,
			FromId:   c.node.id,
			Date:     make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
			Digest:   make([]byte, 0),
			Sigature: make([]byte, 0),
		},
	}
	var reply bool
	for i := 0; i < KConfig.ReqNum; i++ {
		seq := c.node.id*1000 + int64(i)

		c.mu.Lock()
		c.seq2cert[seq] = &ReqCert{
			seq:   seq,
			start: time.Now(),
		}
		c.mu.Unlock()

		args.Req.Seq = seq
		for id, srvCli := range c.id2srvCli {
			Info("send req %d to node %d", seq, id)
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
		seq2cert:  make(map[int64]*ReqCert),
		coch:      make(chan interface{}),
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
