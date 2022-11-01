package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
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
	result     string
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

	// node := GetNode(msg.NodeId)
	// digest := Sha256Digest(msg)
	// ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	// if !ok {
	// 	Warn("ReplyMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
	// 	return nil
	// }
	// measure

	cert := c.getCertOrNew(msg.Seq)

	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := cert.replys[msg.NodeId]
	if ok {
		return nil
	}
	cert.replys[msg.NodeId] = args.Sign
	replyCount := len(cert.replys)

	// Debug("replyCount: %d, f+1: %d", replyCount, KConfig.FalultNum+1)
	if replyCount != KConfig.FaultNum+1 {
		return nil
	}
	c.applyCount++
	take := ToSecond(time.Since(c.start))
	tps := float64(KConfig.BatchTxNum*int(c.applyCount)) / take
	latency := time.Since(cert.start)
	c.sumLatency += latency.Milliseconds()
	avgLatency := float64(c.sumLatency) / float64(c.applyCount)

	c.result = fmt.Sprintf("apply: %d seq: %d take: %.2f tps: %.0f curLa: %d avgLa: %.2f",
		c.applyCount, msg.Seq, take, tps, latency.Milliseconds(), avgLatency)
	Info(c.result)

	<-c.coch
	*reply = true
	return nil
}

func (c *Client) connect() {
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

}

func (c *Client) sendReq() {
	Info("start send req")

	req := &RequestMsg{
		// Operator:  make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
		Timestamp: time.Now().UnixNano(),
		ClientId:  c.node.id,
	}
	// digest := Sha256Digest(req)
	// sign := RsaSignWithSha256(digest, c.node.priKey)
	args := &RequestArgs{
		Req: req,
		// Sign: sign,
	}

	peerNum := len(KConfig.PeerIds)
	c.start = time.Now()
	cnt := 0

	Info("clientNum: %d, reqNum: %d", c.num, KConfig.ReqNum)
	var wg sync.WaitGroup
	for i := 0; i < c.num; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < KConfig.ReqNum; i++ {

				c.coch <- i

				randId := KConfig.PeerIds[rand.Intn(peerNum)]
				srvCli := c.id2srvCli[randId]

				// randId := KConfig.PeerIds[i % peerNum]
				// srvCli := c.id2srvCli[randId]

				reply := &RequestReply{}
				start := time.Now()

				err := srvCli.Call("Server.RequestRpc", args, &reply)
				if err != nil {
					Warn("Server.RequestRpc %d err: %v", randId)
				}
				if !reply.Ok {
					Warn("verify error.")
				}
				seq := reply.Seq
				cert := c.getCertOrNew(seq)

				c.mu.Lock()
				cert.start = start
				// cert.digest = digest
				cnt++
				cnt2 := cnt
				c.mu.Unlock()

				Info("send %d req to node %d, assign seq: %d", cnt2, randId, seq)
			}
		}()
	}
	wg.Wait()

	time.Sleep(time.Second * 3)
	// 通知服务器关闭客户端连接
	Info("close connect with servers")
	for id, srvCli := range c.id2srvCli {
		args := CloseCliCliArgs{
			ClientId: c.node.id,
		}
		var reply bool
		err := srvCli.Call("Server.CloseCliCliRPC", args, &reply)
		if err != nil {
			Warn("Server.RequestRpc %d err: %v", id)
		}
	}
	// 输出结果
	Info("result: \n%s", c.result)

	os.Exit(0)
}

func (c *Client) Start() {
	c.connect()
	time.Sleep(time.Millisecond * 200)
	c.sendReq()
}

func RunClient(clientNum int) {
	client := &Client{
		node:      KConfig.ClientNode,
		id2srvCli: make(map[int64]*rpc.Client),
		seq2cert:  make(map[int64]*CmdCert),
		num:       clientNum,
		coch:      make(chan interface{}, clientNum),
	}
	go client.Start()

	rpc.Register(client)
	rpc.HandleHTTP()
	if err := http.ListenAndServe(client.node.addr, nil); err != nil {
		Error("client listen err: %v", err)
	}
}
