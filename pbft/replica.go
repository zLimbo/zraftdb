package pbft

import (
	"encoding/json"
	"os"
	"time"
)

type Batch struct {
	counts           [5]int
	times            [5]time.Time
	prePrepareMsgNum int
}

func NewBatch() *Batch {
	batch := &Batch{}
	return batch
}

type Stat struct {
	requestNum, prePrepareNum, prepareNum, commitNum, replyNum int64
	prePrepareTime, prepareTime, commitTime                    int64
	batchTimeOkNum                                             int64
	time0, time1, time2, time3, time4                          time.Duration
	time3pcSum, time2pcSum                                     int64
	count3pc, count2pc                                         int64
	execTimeSum, execTimeCnt                                   time.Duration
	signTimeSum, signTimeCnt                                   time.Duration
	verifyPPTimeSum, verifyPPTimeCnt                           time.Duration
	times                                                      []int64
}

func NewStat() *Stat {
	stat := &Stat{
		times: make([]int64, 0),
	}
	return stat
}

type Replica struct {
	node        *Node
	stat        *Stat
	msgCertPool map[int64]*MsgCert
	boostChan   chan int64
	batchPool   map[int64]*Batch
	batchSeq    int64
	curBatch    *Batch
}

func NewReplica(id int64) *Replica {
	pbft := &Replica{
		node:        GetNode(id),
		stat:        NewStat(),
		msgCertPool: make(map[int64]*MsgCert),
		boostChan:   make(chan int64, ChanSize),
		batchPool:   make(map[int64]*Batch),
		batchSeq:    0,
		curBatch:    NewBatch(),
	}
	pbft.batchPool[pbft.batchSeq] = pbft.curBatch
	pbft.curBatch.times[0] = time.Now()
	return pbft
}

func (replica *Replica) Start() {

	go replica.connect()
	go replica.listen()
	go replica.keep()
	go replica.handleMsg()
	go replica.connStatus()
	//go pbft.status()

	if KConfig.ReqNum > 0 && replica.GetIndex() < KConfig.BoostNum {
		go replica.boostReq()
	}

	// 阻塞
	select {}
}

func (replica *Replica) handleMsg() {
	for signMsg := range kSignMsgChan {
		node := GetNode(signMsg.Msg.NodeId)
		start := time.Now()
		if !VerifySignMsg(signMsg, node.pubKey) {
			Error("VerifySignMsg(signMsg, node.pubKey) failed")
		}
		verifyTime := time.Since(start)
		if signMsg.Msg.MsgType == MtPrePrepare {
			replica.stat.verifyPPTimeSum += verifyTime
			replica.stat.verifyPPTimeCnt++
		}

		msg := signMsg.Msg
		Info("batch seq: %d, recvChan size: %d", replica.batchSeq, len(kSignMsgChan))
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

		Info("req=%d, pre-prepare=%d, prepare=%d, commit=%d, reply=%d, recvchan_size=%d, noConnCnt=%d",
			replica.stat.requestNum,
			replica.stat.prePrepareNum,
			replica.stat.prepareNum,
			replica.stat.commitNum,
			replica.stat.replyNum,
			len(kSignMsgChan),
			kNotConnCnt)
		Info("replica.curBatch.prePrepareMsgNum=%d", replica.curBatch.prePrepareMsgNum)
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
	Trace("msg.seq=%d, tx.size=%d", msg.Seq, len(msg.Txs))
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.Req != nil {
		Trace("this request msg[seq=%d] has been accepted", msg.Seq)
		return
	}

	msgCert.Seq = msg.Seq
	msgCert.Req = msg
	msgCert.SendPrePrepare = WaitSend

	msgCert.Time = time.Now().UnixNano()
	msgCert.PrePrepareTime = time.Now().UnixNano()

	replica.curBatch.times[1] = time.Now()

	Info("[START] pre-prepare | seq=%d", msgCert.Seq)

	replica.sendPrePrepare(msgCert)
}

func (replica *Replica) handlePrePrepare(msg *Message) {
	Trace("msg.seq=%d, tx.size=%d", msg.Seq, len(msg.Txs))
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

	msgCert.Time = time.Now().UnixNano()
	msgCert.PrepareTime = time.Now().UnixNano()
	Info("[START] prepare | seq=%d", msgCert.Seq)

	replica.sendPrepare(msgCert)
}

func (replica *Replica) handlePrepare(msg *Message) {
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.SendCommit == HasSend {
		return
	}
	Trace("msg.seq=%d, msg.nodeId=%d", msg.Seq, msg.NodeId)
	replica.recvPrepareMsg(msgCert, msg)
	replica.maybeSendCommit(msgCert)
}

func (replica *Replica) handleCommit(msg *Message) {
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.SendReply == HasSend {
		return
	}
	Trace("msg.seq=%d, msg.nodeId=%d", msg.Seq, msg.NodeId)
	replica.recvCommitMsg(msgCert, msg)
	replica.maybeSendReply(msgCert)
}

func (replica *Replica) recvPrepareMsg(msgCert *MsgCert, msg *Message) {
	// count := 1
	// for _, preMsg := range msgCert.Prepares {
	// 	if preMsg.NodeId == msg.NodeId {
	// 		return
	// 	}
	// 	if preMsg.Digest == msg.Digest {
	// 		count++
	// 	}
	// }
	msgCert.Prepares = append(msgCert.Prepares, msg)
	count := len(msgCert.Prepares)
	Trace("same prepare msg count: %d", count)
	if count >= 2*KConfig.FalultNum {
		msgCert.SendCommit = WaitSend
	}
}

func (replica *Replica) recvCommitMsg(msgCert *MsgCert, msg *Message) {
	// count := 1
	// for _, preMsg := range msgCert.Commits {
	// 	if preMsg.NodeId == msg.NodeId {
	// 		return
	// 	}
	// 	if preMsg.Digest == msg.Digest {
	// 		count++
	// 	}
	// }
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
	replica.broadcast(signMsg, msgCert)
	msgCert.SendPrePrepare = HasSend
	Trace("[pre-prepare] msg has been sent.")

	msgCert.PrePrepareTime = time.Now().UnixNano() - msgCert.PrePrepareTime
	Info("[END] pre-prepare | seq=%d", msgCert.Seq)

	msgCert.PrepareTime = time.Now().UnixNano()
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
	replica.broadcast(signMsg, msgCert)
	msgCert.SendPrepare = HasSend
	replica.stat.prePrepareNum++

	//pbft.curBatch.counts[2]++
	//if pbft.curBatch.counts[2] == 2*f+1 {
	//	pbft.curBatch.times[2] = time.Now()
	//}

	Trace("[prepare] msg has been sent.")
	replica.maybeSendCommit(msgCert)
}

func (replica *Replica) maybeSendCommit(msgCert *MsgCert) {
	if msgCert.SendPrepare != HasSend || msgCert.SendCommit != WaitSend {
		return
	}
	replica.stat.prepareNum++

	msgCert.PrepareTime = time.Now().UnixNano() - msgCert.PrepareTime

	Info("[END] prepare | seq=%d", msgCert.Seq)

	//pbft.curBatch.counts[3]++
	//if pbft.curBatch.counts[3] == 2*f+1 {
	//	pbft.curBatch.times[3] = time.Now()
	//}

	msgCert.CommitTime = time.Now().UnixNano()
	Info("[START] commit | seq=%d", msgCert.Seq)

	commitMsg := &Message{
		MsgType:   MtCommit,
		Seq:       msgCert.Seq,
		NodeId:    replica.node.id,
		Timestamp: time.Now().UnixNano(),
	}

	replica.recvCommitMsg(msgCert, commitMsg)
	signMsg := replica.signMsg(commitMsg)
	replica.broadcast(signMsg, msgCert)
	msgCert.SendCommit = HasSend
	Trace("commit msg has been sent.")
	replica.maybeSendReply(msgCert)
}

func (replica *Replica) maybeSendReply(msgCert *MsgCert) {
	if msgCert.SendCommit != HasSend || msgCert.SendReply != WaitSend {
		return
	}

	msgCert.Time = time.Now().UnixNano() - msgCert.Time
	msgCert.CommitTime = time.Now().UnixNano() - msgCert.CommitTime
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
	// go PostJson(ClientUrl+"/getReply", jsonMsg)
	msgCert.SendReply = HasSend

	Trace("reply msg has been sent, seq=%d", msgCert.Seq)
	Info("[END] reply | seq=%d, reply_count=%d", msgCert.Seq, replica.stat.replyNum)

	replica.stat.replyNum++
	replica.finalize(msgCert)
}

func (replica *Replica) finalize(msgCert *MsgCert) {
	// pbft.showTime(msgCert)

	replica.curBatch.counts[4]++
	if replica.curBatch.counts[4] == KConfig.BoostNum {
		replica.curBatch.times[4] = time.Now()
		Info("[END] batch | seq=%d", replica.batchSeq)

		// replica.showBatchTime()
		replica.exec(KConfig.BoostNum)
		replica.curBatch = &Batch{}
		replica.curBatch.times[0] = time.Now()
		replica.batchSeq += 1
		replica.batchPool[replica.batchSeq] = replica.curBatch
		replica.boostChan <- replica.batchSeq

		Info("[START] batch | seq=%d", replica.batchSeq)
	}

	// replica.showTime(msgCert)
	replica.clearCert(msgCert)
}

func (replica *Replica) exec(num int) {
	Info("[START] execute")
	start := time.Now()
	sum := int64(0)
	for j := 0; j < num; j++ {
		for i := int64(0); i < ExecNum; i++ {
			sum += i
		}
	}
	Trace("Exec result: sum=%d", sum)
	Info("[END] execute")
	execTime := time.Since(start)
	replica.stat.execTimeSum += execTime
	replica.stat.execTimeCnt++

	Info("execTime=%d", execTime)
}

func (replica *Replica) clearCert(msgCert *MsgCert) {
	msgCert.Req = nil
	msgCert.PrePrepare = nil
	msgCert.Prepares = nil
	msgCert.Commits = nil
}

func (replica *Replica) broadcast(signMsg *SignMessage, msgCert *MsgCert) {
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

func (replica *Replica) signMsg(msg *Message) *SignMessage {
	start := time.Now()
	signMsg := SignMsg(msg, replica.node.priKey)
	signTime := time.Since(start)
	if msg.MsgType == MtPrePrepare {
		replica.stat.signTimeSum += signTime
		replica.stat.signTimeCnt++
	}
	return signMsg
}

func (replica *Replica) boostReq() {

	if KConfig.ReqNum <= 0 {
		return
	}

	req := &Message{
		MsgType:   MtRequest,
		Seq:       1,
		NodeId:    replica.node.id,
		Timestamp: time.Now().UnixNano(),
		Txs:       &BatchTx{},
	}
	SignRequest(req, replica.node.priKey)
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		Warn("json.Marshal(req), err: %v", err)
	}
	Info("req.size=%.2f, ReqNum=%d, boostChan=%d", float64(len(jsonBytes))/MBSize, KConfig.ReqNum, len(replica.boostChan))
	//time.Sleep(time.Duration(boostDelay) * time.Millisecond)

	replica.boostChan <- 0

	Info("[START] batch | seq=%d", replica.batchSeq)
	prefix := replica.node.id * 10000

	time.Sleep(time.Millisecond * time.Duration(KConfig.StartDelay))
	start := time.Now()

	for batchSeq := range replica.boostChan {
		if batchSeq == int64(KConfig.ReqNum) {
			break
		}
		req.Seq = prefix + batchSeq
		req.Timestamp = time.Now().UnixNano()
		// signMsg := replica.signMsg(req)
		// kSignMsgChan <- signMsg

		msgCert := replica.getMsgCert(req.Seq)
		msgCert.Req = req
		msgCert.SendPrePrepare = WaitSend

		// 直接发送pre-prepare消息
		replica.sendPrePrepare(msgCert)
	}
	Info("all request finish | KConfig.ReqNum=%d, spendTime=%d", KConfig.ReqNum, time.Since(start))

	time.Sleep(time.Second * 60)
	for _, node := range KConfig.Id2Node {
		node.connMgr.closeTcpConn()
	}
	Info("done, exit")
	os.Exit(1)
}
