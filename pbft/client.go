package pbft

import (
	"log"
	"time"
)

type Client struct {
	Pbft
	replyCertPool map[int64]*ReplyCert
	applyNum      int
	startTime     time.Time
}

func NewClient() *Client {
	client := &Client{
		replyCertPool: make(map[int64]*ReplyCert),
	}
	client.node = ClientNode
	return client
}

func (client *Client) Start() {
	go client.listen()
	go client.handleReplyMsg()

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
	time.Sleep(time.Millisecond * ClientDelay)
	client.startTime = time.Now()
	for signMsg := range recvChan {
		// log.Println("handle reply:", msg)
		node := GetNode(signMsg.Msg.NodeId)
		if !VerifySignMsg(signMsg, node.pubKey) {
			log.Println("#### verify failed!")
			continue
		}
		msg := signMsg.Msg
		if msg.MsgType != MtReply {
			log.Println("it's not reply!")
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
		log.Printf("\033[32m[Reply]\033[0m msg seq=%d node_id=%d, count=%d\n", msg.Seq, msg.NodeId, count)
		if count < f+1 {
			continue
		}
		cert.CanApply = true
		client.applyNum++
		spend := time.Since(client.startTime)
		log.Printf("[time] apply num=%d\tspend time=%0.2fs\n", client.applyNum, ToSecond(spend))
	}
}
