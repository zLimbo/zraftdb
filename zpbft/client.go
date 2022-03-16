package main

import (
	"math/rand"
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
	num        int
}

func (c *Client) getCertOrNew(seq int64) *CmdCert {
	c.mu.Lock()
	defer c.mu.Unlock()
	cert, ok := c.seq2cert[seq]
	if !ok {
		cert = &CmdCert{
			seq:    seq,
			start:  time.Now(),
			replys: make(map[int64][]byte),
		}
		c.seq2cert[seq] = cert
	}
	return cert
}

func (c *Client) ReplyRpc(args *ReplyArgs, reply *bool) error {
	msg := args.Msg
	Debug("ReplyRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)
	*reply = false

	if msg.ClientId != c.node.id {
		Warn("ReplyMsg msg.ClientId == c.node.id")
		return nil
	}

	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("ReplyMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}
	// measure

	cert := c.getCertOrNew(msg.Seq)

	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok = cert.replys[msg.NodeId]
	if ok {
		return nil
	}
	cert.replys[msg.NodeId] = args.Sign
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
		c.applyCount, msg.Seq, take, tps, latency.Milliseconds(), avgLatency)

	// c.coch <- args.Seq
	*reply = true
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
					Warn("connect %d error: %v", node.addr, err)
					ok = false
				} else {
					c.id2srvCli[id] = cli
				}
			}
		}
	}
	Info("\n== connect success ==")
	time.Sleep(time.Millisecond * 200)
	coch <- 1
}

func (c *Client) sendReq(coch <-chan interface{}) {
	<-coch
	Info("start send req")

	req := &RequestMsg{
		Operator:  make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
		Timestamp: time.Now().UnixNano(),
		ClientId:  c.node.id,
	}
	digest := Sha256Digest(req)
	sign := RsaSignWithSha256(digest, c.node.priKey)
	args := &RequestArgs{
		Req:  req,
		Sign: sign,
	}

	peerNum := len(KConfig.PeerIds)
	c.start = time.Now()
	cnt := 0

	Info("clientNum: %d, reqNum: %d", c.num, KConfig.ReqNum)
	for i := 0; i < c.num; i++ {
		go func() {
			for i := 0; i < KConfig.ReqNum; i++ {
				randId := KConfig.PeerIds[rand.Intn(peerNum)]
				srvCli := c.id2srvCli[randId]

				reply := &RequestReply{}
				start := time.Now()

				err := srvCli.Call("Server.RequestRpc", args, &reply)
				if err != nil {
					Warn("Server.RequestRpc err: %v", err)
				}
				if !reply.Ok {
					Warn("verify error.")
				}
				seq := reply.Seq
				cert := c.getCertOrNew(seq)

				c.mu.Lock()
				cert.start = start
				cert.digest = digest
				cnt++
				cnt2 := cnt
				c.mu.Unlock()

				Info("send %d req to node %d, assign seq: %d", cnt2, randId, seq)
			}
		}()
	}
}

func RunClient(clientNum int) {
	client := &Client{
		node:      KConfig.ClientNode,
		id2srvCli: make(map[int64]*rpc.Client),
		seq2cert:  make(map[int64]*CmdCert),
		coch:      make(chan interface{}, 100),
		num:       clientNum,
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
