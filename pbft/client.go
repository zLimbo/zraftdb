package pbft

import (
	"os"
	"time"
)

type Client struct {
	Replica
	replyCertPool map[int64]*ReplyCert
	nApply        int
}

func NewClient() *Client {
	client := &Client{
		replyCertPool: make(map[int64]*ReplyCert),
	}
	client.node = KConfig.ClientNode
	return client
}

func (client *Client) Start() {
	go client.listen()
	go client.handleReplyMsg()
	// go client.connStatus()

	select {}
}

func (client *Client) getReplyCert(seq int64) *ReplyCert {
	cert, ok := client.replyCertPool[seq]
	if !ok {
		cert = NewReplyCert(seq)
		client.replyCertPool[seq] = cert
	}
	return cert
}

func (client *Client) handleReplyMsg() {
	time.Sleep(time.Millisecond * time.Duration(KConfig.StartDelay))

	nTotalReq := KConfig.IpNum * KConfig.ProcessNum * KConfig.ReqNum
	start := time.Now()

	for signMsg := range kSignMsgChan {
		// Trace("handle reply:", msg)
		node := GetNode(signMsg.Msg.NodeId)
		if !VerifySignMsg(signMsg, node.pubKey) {
			Warn("VerifySignMsg(signMsg, node.pubKey) failed")
			continue
		}
		msg := signMsg.Msg
		if msg.MsgType != MtReply {
			Warn("it's not reply!")
			continue
		}
		cert := client.getReplyCert(msg.Seq)
		if cert.CanApply {
			continue
		}
		count := 1
		for _, preMsg := range cert.Replys {
			if preMsg.NodeId == msg.NodeId {
				return
			}
			count++
		}
		cert.Replys = append(cert.Replys, msg)
		Debug("msg.seq=%d, node.id=%d, count=%d", msg.Seq, msg.NodeId, count)
		if count < KConfig.FalultNum+1 {
			continue
		}
		cert.CanApply = true
		client.nApply++
		spend := ToSecond(time.Since(start))
		tps := float64(client.nApply*KConfig.BatchTxNum) / spend
		traffic := float64(client.nApply*KConfig.BatchTxNum*KConfig.TxSize) / MBSize / spend
		Info("nApply=%d, spend=%.2f(s), tps=%.2f, traffic=%.2f(MB/s)", client.nApply, spend, tps, traffic)

		if client.nApply == nTotalReq {
			break
		}
	}
	time.Sleep(time.Second * 3)
	for _, node := range KConfig.Id2Node {
		node.connMgr.closeTcpConn()
	}
	Info("done, exit")
	os.Exit(1)
}

// func getReqSize() float64 {
// 	req := &Message{
// 		MsgType:   MtRequest,
// 		Seq:       1,
// 		NodeId:    0,
// 		Timestamp: time.Now().UnixNano(),
// 		Tx:        make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
// 	}
// 	SignMsg(req, node.priKey)
// 	jsonBytes, err := json.Marshal(req)
// 	if err != nil {
// 		Warn("json.Marshal(req), err: %v", err)
// 	}
// 	return float64(len(jsonBytes)) / MBSize
// }
