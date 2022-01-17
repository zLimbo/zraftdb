package pbft

import (
	"encoding/json"
	"os"
	"time"
)

type Batch struct {
	count int
}

func NewBatch() *Batch {
	batch := &Batch{}
	return batch
}

type Stat struct {
	requestNum, prePrepareNum, prepareNum, commitNum, replyNum int64
}

func NewStat() *Stat {
	stat := &Stat{}
	return stat
}

type Replica struct {
	node        *Node
	stat        *Stat
	boostChan   chan int64
	msgCertPool map[int64]*MsgCert
	batchPool   map[int64]*Batch
	gossipPool  map[string]bool
	batchSeq    int64
	curBatch    *Batch
}

func NewReplica(id int64) *Replica {
	pbft := &Replica{
		node:        GetNode(id),
		stat:        NewStat(),
		boostChan:   make(chan int64, ChanSize),
		msgCertPool: make(map[int64]*MsgCert),
		gossipPool:  make(map[string]bool),
		batchPool:   make(map[int64]*Batch),
		batchSeq:    0,
		curBatch:    NewBatch(),
	}
	pbft.batchPool[pbft.batchSeq] = pbft.curBatch
	return pbft
}

func (replica *Replica) Start() {

	go replica.connect()
	go replica.listen()
	go replica.keep()
	go replica.handleMsg()
	go replica.connStatus()
	//go pbft.status()

	if KConfig.ReqNum > 0 && GetIndex(replica.node.id) < KConfig.BoostNum {
		go replica.boostReq()
	}

	// 阻塞
	select {}
}

func (replica *Replica) handleMsg() {
	for signMsg := range kSignMsgChan {

		// 若有配置，采用 gossip 转发
		if KConfig.EnableGossip {
			ok := replica.gossip(signMsg)
			// 如果未进行gossip发送，说明该消息已经被本节点处理
			if !ok {
				continue
			}
		}

		node := GetNode(signMsg.Msg.NodeId)
		if !VerifySignMsg(signMsg, node.pubKey) {
			Error("VerifySignMsg(signMsg, node.pubKey) failed")
		}

		msg := signMsg.Msg
		Info("batch seq: %d", replica.batchSeq)
		switch msg.MsgType {
		case MtRequest:
			replica.handleRequest(msg)
		case MtPrePrepare:
			replica.handlePrePrepare(msg)
		case MtPrepare:
			replica.handlePrepare(msg)
		case MtCommit:
			replica.handleCommit(msg)
		}

		Info("requestNum=%d, prePrepareNum=%d, prepareNum=%d, commitNum=%d, replyNum=%d, kSignMsgChan.size=%d, kNotConnCnt=%d",
			replica.stat.requestNum,
			replica.stat.prePrepareNum,
			replica.stat.prepareNum,
			replica.stat.commitNum,
			replica.stat.replyNum,
			len(kSignMsgChan),
			kNotConnCount)
	}
}

func (replica *Replica) getMsgCert(seq int64) *MsgCert {
	msgCert, ok := replica.msgCertPool[seq]
	if !ok {
		msgCert = NewMsgCert()
		replica.msgCertPool[seq] = msgCert
		msgCert.Seq = seq
	}
	return msgCert
}

func (replica *Replica) handleRequest(msg *Message) {
	Trace("msg.seq=%d, tx.size=%d", msg.Seq, len(msg.Tx))
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.Req != nil {
		Trace("this request msg[seq=%d] has been accepted", msg.Seq)
		return
	}

	msgCert.Seq = msg.Seq
	msgCert.Req = msg
	msgCert.SendPrePrepare = WaitSend

	Info("[START] pre-prepare | seq=%d", msgCert.Seq)

	replica.sendPrePrepare(msgCert)
}

func (replica *Replica) handlePrePrepare(msg *Message) {
	Trace("msg.seq=%d, tx.size=%d", msg.Seq, len(msg.Tx))
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.PrePrepare != nil {
		Trace("this pre-prepare msg[seq=%d] has been accepted", msg.Seq)
		return
	}
	if msgCert.SendPrepare == HasSend {
		return
	}

	msgCert.Seq = msg.Seq
	msgCert.PrePrepare = msg
	msgCert.SendPrepare = WaitSend

	Info("[START] prepare | seq=%d", msgCert.Seq)

	replica.sendPrepare(msgCert)
}

func (replica *Replica) handlePrepare(msg *Message) {
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.SendCommit == HasSend {
		return
	}
	Trace("msg.Seq=%d, msg.NodeId=%d", msg.Seq, msg.NodeId)
	replica.recvPrepareMsg(msgCert, msg)
	replica.maybeSendCommit(msgCert)
}

func (replica *Replica) handleCommit(msg *Message) {
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.SendReply == HasSend {
		return
	}
	Trace("msg.Seq=%d, msg.NodeId=%d", msg.Seq, msg.NodeId)
	replica.recvCommitMsg(msgCert, msg)
	replica.maybeSendReply(msgCert)
}

func (replica *Replica) recvPrepareMsg(msgCert *MsgCert, msg *Message) {
	msgCert.Prepares = append(msgCert.Prepares, msg)
	count := len(msgCert.Prepares)
	Trace("same prepare msg count: %d", count)
	if count >= 2*KConfig.FalultNum {
		msgCert.SendCommit = WaitSend
	}
}

func (replica *Replica) recvCommitMsg(msgCert *MsgCert, msg *Message) {
	msgCert.Commits = append(msgCert.Commits, msg)
	count := len(msgCert.Commits)
	Trace("same commit msg count: %d", count)
	if count >= 2*KConfig.FalultNum+1 {
		msgCert.SendReply = WaitSend
	}
}

func (replica *Replica) sendPrePrepare(msgCert *MsgCert) {
	prePrepareMsg := &Message{
		MsgType:   MtPrePrepare,
		Seq:       msgCert.Seq,
		NodeId:    replica.node.id,
		Timestamp: time.Now().UnixNano(),
		Req:       msgCert.Req,
	}
	signMsg := replica.signMsg(prePrepareMsg)
	replica.broadcast(signMsg)
	msgCert.SendPrePrepare = HasSend

	Trace("[pre-prepare] msg has been sent.")
	Info("[END] pre-prepare | seq=%d", msgCert.Seq)

	msgCert.SendPrepare = HasSend
	Info("[START] prepare | seq=%d", msgCert.Seq)

	replica.maybeSendCommit(msgCert)
}

func (replica *Replica) sendPrepare(msgCert *MsgCert) {
	prepareMsg := &Message{
		MsgType:   MtPrepare,
		Seq:       msgCert.Seq,
		NodeId:    replica.node.id,
		Timestamp: time.Now().UnixNano(),
	}
	replica.recvPrepareMsg(msgCert, prepareMsg)
	signMsg := replica.signMsg(prepareMsg)
	replica.broadcast(signMsg)
	msgCert.SendPrepare = HasSend
	replica.stat.prePrepareNum++
	Trace("[prepare] msg has been sent.")

	replica.maybeSendCommit(msgCert)
}

func (replica *Replica) maybeSendCommit(msgCert *MsgCert) {
	if msgCert.SendPrepare != HasSend || msgCert.SendCommit != WaitSend {
		return
	}
	replica.stat.prepareNum++

	Info("[END] prepare | seq=%d", msgCert.Seq)
	Info("[START] commit | seq=%d", msgCert.Seq)

	commitMsg := &Message{
		MsgType:   MtCommit,
		Seq:       msgCert.Seq,
		NodeId:    replica.node.id,
		Timestamp: time.Now().UnixNano(),
	}
	replica.recvCommitMsg(msgCert, commitMsg)
	signMsg := replica.signMsg(commitMsg)
	replica.broadcast(signMsg)
	msgCert.SendCommit = HasSend
	Trace("commit msg has been sent.")

	replica.maybeSendReply(msgCert)
}

func (replica *Replica) maybeSendReply(msgCert *MsgCert) {
	if msgCert.SendCommit != HasSend || msgCert.SendReply != WaitSend {
		return
	}
	replica.stat.commitNum++

	Info("[END] commit | seq=%d", msgCert.Seq)
	Info("[START] reply | seq=%d", msgCert.Seq)

	replyMsg := &Message{
		MsgType:   MtReply,
		Seq:       msgCert.Seq,
		NodeId:    replica.node.id,
		Timestamp: time.Now().UnixNano(),
	}
	signMsg := replica.signMsg(replyMsg)
	signMsgBytes, err := json.Marshal(signMsg)
	if err != nil {
		Error("json.Marshal(signMsg), err: %v", err)
	}
	KConfig.ClientNode.connMgr.sendChan <- signMsgBytes
	msgCert.SendReply = HasSend

	Trace("reply msg has been sent, seq=%d", msgCert.Seq)
	Info("[END] reply | seq=%d, reply_count=%d", msgCert.Seq, replica.stat.replyNum)

	replica.stat.replyNum++
	replica.finalize(msgCert)
}

func (replica *Replica) finalize(msgCert *MsgCert) {

	replica.curBatch.count++
	if replica.curBatch.count == KConfig.BoostNum {
		Info("[END] batch | seq=%d", replica.batchSeq)

		replica.exec(KConfig.BoostNum)
		replica.curBatch = &Batch{}
		replica.batchSeq += 1
		replica.batchPool[replica.batchSeq] = replica.curBatch
		replica.boostChan <- replica.batchSeq

		replica.boostChan <- replica.batchSeq + 1
	}
	// 清理，释放内存
	replica.clearCert(msgCert)
}

func (replica *Replica) exec(num int) {
	Info("[START] execute")
	start := time.Now()
	sum := int64(0)
	for j := 0; j < KConfig.BoostNum; j++ {
		for i := 0; i < KConfig.ExecNum; i++ {
			sum += int64(i)
		}
	}
	execTime := time.Since(start)
	Trace("Exec result: sum=%d", sum)
	Info("[END] execute")
	Info("execTime=%d", execTime)
}

func (replica *Replica) clearCert(msgCert *MsgCert) {
	msgCert.Req = nil
	msgCert.PrePrepare = nil
	msgCert.Prepares = nil
	msgCert.Commits = nil
}

func (replica *Replica) broadcast(signMsg *SignMessage) {
	// 如果配置gossip，则使用gossip
	if KConfig.EnableGossip {
		replica.gossip(signMsg)
		return
	}

	signMsgBytes, err := json.Marshal(signMsg)
	if err != nil {
		Error("json.Marshal(signMsg), err: %v", err)
	}
	for _, node := range KConfig.Id2Node {
		if node.id == replica.node.id {
			continue
		}
		node.connMgr.sendChan <- signMsgBytes
	}
}

func (replica *Replica) gossip(signMsg *SignMessage) bool {
	// 如果已经发送过，则不发送
	msgId := GetMsgId(signMsg.Msg)
	if replica.gossipPool[msgId] {
		return false
	}
	replica.gossipPool[msgId] = true

	signMsg.NodeId = replica.node.id
	signMsgBytes, err := json.Marshal(signMsg)
	if err != nil {
		Error("json.Marshal(signMsg), err: %v", err)
	}

	// 随机选 KConfig.GossipNum 个节点发送
	// rand.Seed(time.Now().Unix())
	// ids := KConfig.PeerIds[:]
	// rand.Shuffle(len(ids), func(i, j int) {
	// 	ids[i], ids[j] = ids[j], ids[i]
	// })
	// count := 0
	// for _, id := range ids {
	// 	node := KConfig.Id2Node[id]
	// 	if id == replica.node.id || id == fromId || id == signMsg.Msg.NodeId {
	// 		continue
	// 	}
	// 	node.connMgr.sendChan <- signMsgBytes
	// 	count++
	// 	if count == KConfig.GossipNum {
	// 		break
	// 	}
	// }

	// 路由表
	for _, id := range KConfig.RouteMap[replica.node.id] {
		node := KConfig.Id2Node[id]
		if id == signMsg.Msg.NodeId {
			continue
		}
		node.connMgr.sendChan <- signMsgBytes
	}
	return true
}

func (replica *Replica) signMsg(msg *Message) *SignMessage {
	signMsg := SignMsg(msg, replica.node.priKey)
	return signMsg
}

// 自己发请求
func (replica *Replica) boostReq() {

	if KConfig.ReqNum <= 0 {
		return
	}

	req := &Message{
		MsgType:   MtRequest,
		Seq:       1,
		NodeId:    replica.node.id,
		Timestamp: time.Now().UnixNano(),
		Tx:        make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
	}
	SignMsg(req, replica.node.priKey)
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		Warn("json.Marshal(req), err: %v", err)
	}
	Info("req.size=%.2fMB, ReqNum=%d, boostChan=%d", float64(len(jsonBytes))/MBSize, KConfig.ReqNum, len(replica.boostChan))

	replica.boostChan <- 0

	
	prefix := replica.node.id * 1000

	time.Sleep(time.Millisecond * time.Duration(KConfig.StartDelay))
	start := time.Now()

	for replica.batchSeq = range replica.boostChan {
		if replica.batchSeq == int64(KConfig.ReqNum) {
			break
		}

		Info("[START] batch | seq=%d", replica.batchSeq)

		req.Seq = prefix + replica.batchSeq
		req.Timestamp = time.Now().UnixNano()
		// signMsg := replica.signMsg(req)
		// kSignMsgChan <- signMsg

		msgCert := replica.getMsgCert(req.Seq)
		msgCert.Req = req
		msgCert.SendPrePrepare = WaitSend

		// 直接发送pre-prepare消息
		replica.sendPrePrepare(msgCert)
	}
	spend := ToSecond(time.Since(start))
	Info("all request finish | KConfig.ReqNum=%d, spendTime=%.2f", KConfig.ReqNum, spend)

	// 如果客户端关闭连接则退出
	for {
		if KConfig.ClientNode.connMgr.getTcpConn() == nil {
			break
		}
		time.Sleep(time.Second * 10)
	}

	for _, node := range KConfig.Id2Node {
		node.connMgr.closeTcpConn()
	}

	Info("done, exit")
	os.Exit(1)
}
