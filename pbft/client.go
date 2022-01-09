package pbft

import (
	"time"
)

type Client struct {
	Replica
	replyCertPool map[int64]*ReplyCert
	applyNum      int
	startTime     time.Time
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
	go client.connStatus()

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
	client.startTime = time.Now()
	for signMsg := range kSignMsgChan {
		// Info("handle reply:", msg)
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
			// if preMsg.Digest == msg.Digest {
			// 	count++
			// }
		}
		cert.Replys = append(cert.Replys, msg)
		Trace("msg.seq=%d, node.id=%d, count=%d", msg.Seq, msg.NodeId, count)
		if count < KConfig.FalultNum+1 {
			continue
		}
		cert.CanApply = true
		client.applyNum++
		spend := time.Since(client.startTime)
		Info("== applyNum=%d, spend=%v", client.applyNum, spend)
	}
}
