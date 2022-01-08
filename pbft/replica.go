package pbft

import (
	"encoding/json"
	"fmt"
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

	if KConfig.ReqNum > 0 && GetIndex(replica.node.ip) < KConfig.BoostNum {
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
			Panic("VerifySignMsg(signMsg, node.pubKey) failed")
		}
		verifyTime := time.Since(start)
		if signMsg.Msg.MsgType == MtPrePrepare {
			replica.stat.verifyPPTimeSum += verifyTime
			replica.stat.verifyPPTimeCnt++
		}

		msg := signMsg.Msg
		Info("batch seq: %d, recvChan size: %d", replica.batchSeq, len(kSignMsgChan))
		switch msg.MsgType {
		case MtProposal:
			replica.handleProposal(msg)
		case MtRequest:
			replica.handleRequest(msg)
		case MtPrePrepare:
			replica.handlePrePrepare(msg)
		case MtPrepare:
			replica.handlePrepare(msg)
		case MtCommit:
			replica.handleCommit(msg)
		}

		Info("req=%d, pre-prepare=%d, prepare=%d, commit=%d, reply=%d, recvchan_size=%d, noConnCnt=%d\n",
			replica.stat.requestNum,
			replica.stat.prePrepareNum,
			replica.stat.prepareNum,
			replica.stat.commitNum,
			replica.stat.replyNum,
			len(kSignMsgChan),
			kNotConnCnt)
		Info("recv pre-prepare num:", replica.curBatch.prePrepareMsgNum, "]")
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
	Info("msg.seq: %d tx.num: %d", msg.Seq, len(msg.Txs))
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.Req != nil {
		Info("The request[seq=%d] has been accepted", msg.Seq)
		return
	}

	msgCert.Seq = msg.Seq
	msgCert.Req = msg
	msgCert.SendPrePrepare = WaitSend

	msgCert.Time = time.Now().UnixNano()
	msgCert.PrePrepareTime = time.Now().UnixNano()

	replica.curBatch.times[1] = time.Now()

	LogStageStart(fmt.Sprintf("seq=%d pre-prepare", msgCert.Seq))

	replica.sendPrePrepare(msgCert)
}

func (replica *Replica) handlePrePrepare(msg *Message) {
	Info("<node handlePrePrepareMsg> msg seq=", msg.Seq, "tx num", len(msg.Txs))
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.PrePrepare != nil {
		Info("The pre-prepare(seq=%d) has been accepted!\n", msg.Seq)
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
	LogStageStart(fmt.Sprintf("seq=%d prepare", msgCert.Seq))

	replica.sendPrepare(msgCert)
}

func (replica *Replica) handlePrepare(msg *Message) {
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.SendCommit == HasSend {
		return
	}
	Info("<node handlePrepareMsg> msg seq=%d nodeId=%d\n", msg.Seq, msg.NodeId)
	Info("\t\033[32mtime:\033[0m", time.Duration(time.Now().UnixNano()-msgCert.PrePrepareTime))
	replica.recvPrepareMsg(msgCert, msg)
	replica.maybeSendCommit(msgCert)
}

func (replica *Replica) handleCommit(msg *Message) {
	msgCert := replica.getMsgCert(msg.Seq)
	if msgCert.SendReply == HasSend {
		return
	}
	Info("<node handleCommitMsg> msg seq=%d nodeId=%d\n", msg.Seq, msg.NodeId)
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
	Info("same prepare msg count=%d\n", count)
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
	Info("same commit msg count=%d\n", count)
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
	Info("[pre-prepare] msg has been sent.")

	msgCert.PrePrepareTime = time.Now().UnixNano() - msgCert.PrePrepareTime
	LogStageEnd(fmt.Sprintf("seq=%d pre-prepare", msgCert.Seq))

	msgCert.PrepareTime = time.Now().UnixNano()
	msgCert.SendPrepare = HasSend
	LogStageStart(fmt.Sprintf("seq=%d prepare", msgCert.Seq))
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
	Info("<broadcast> prepare")
	replica.broadcast(signMsg, msgCert)
	msgCert.SendPrepare = HasSend
	replica.stat.prePrepareNum++

	//pbft.curBatch.counts[2]++
	//if pbft.curBatch.counts[2] == 2*f+1 {
	//	pbft.curBatch.times[2] = time.Now()
	//}

	Info("[prepare] msg has been sent.")
	replica.maybeSendCommit(msgCert)
}

func (replica *Replica) maybeSendCommit(msgCert *MsgCert) {
	if msgCert.SendPrepare != HasSend || msgCert.SendCommit != WaitSend {
		return
	}
	replica.stat.prepareNum++

	msgCert.PrepareTime = time.Now().UnixNano() - msgCert.PrepareTime
	LogStageEnd(fmt.Sprintf("seq=%d prepare", msgCert.Seq))

	//pbft.curBatch.counts[3]++
	//if pbft.curBatch.counts[3] == 2*f+1 {
	//	pbft.curBatch.times[3] = time.Now()
	//}

	msgCert.CommitTime = time.Now().UnixNano()
	LogStageStart(fmt.Sprintf("seq=%d commit", msgCert.Seq))

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
	Info("[commit] msg has been sent.")
	replica.maybeSendReply(msgCert)
}

func (replica *Replica) maybeSendReply(msgCert *MsgCert) {
	if msgCert.SendCommit != HasSend || msgCert.SendReply != WaitSend {
		return
	}

	msgCert.Time = time.Now().UnixNano() - msgCert.Time
	msgCert.CommitTime = time.Now().UnixNano() - msgCert.CommitTime
	replica.stat.commitNum++
	LogStageEnd(fmt.Sprintf("seq=%d commit", msgCert.Seq))

	LogStageStart(fmt.Sprintf("seq=%d reply", msgCert.Seq))
	replyMsg := &Message{
		MsgType:   MtReply,
		Seq:       msgCert.Seq,
		NodeId:    replica.node.id,
		Timestamp: time.Now().UnixNano(),
	}
	signMsg := replica.signMsg(replyMsg)
	signMsgBytes, err := json.Marshal(signMsg)
	if err != nil {
		Panic("json.Marshal(signMsg), err: %v", err)
	}
	KConfig.ClientNode.connMgr.sendChan <- signMsgBytes
	// go PostJson(ClientUrl+"/getReply", jsonMsg)
	msgCert.SendReply = HasSend

	Info("[reply] msg has been sent, seq=%d\n", msgCert.Seq)
	LogStageEnd(fmt.Sprintf("seq=%d reply reply_count=%d", msgCert.Seq, replica.stat.replyNum))
	replica.stat.replyNum++

	replica.finalize(msgCert)
}

func (replica *Replica) finalize(msgCert *MsgCert) {
	// pbft.showTime(msgCert)

	replica.curBatch.counts[4]++
	if replica.curBatch.counts[4] == KConfig.BoostNum {
		replica.curBatch.times[4] = time.Now()
		LogStageEnd(fmt.Sprintf("Batch %d", replica.batchSeq))

		replica.showBatchTime()
		//pbft.exec(BoostNum)
		replica.curBatch = &Batch{}
		replica.curBatch.times[0] = time.Now()
		replica.batchSeq += 1
		replica.batchPool[replica.batchSeq] = replica.curBatch
		replica.boostChan <- replica.batchSeq

		LogStageStart(fmt.Sprintf("Batch %d", replica.batchSeq))
	}

	replica.showTime(msgCert)
	replica.clearCert(msgCert)
}

func (replica *Replica) exec(num int) {
	LogStageStart("Exec")
	start := time.Now()
	sum := int64(0)
	for j := 0; j < num; j++ {
		for i := int64(0); i < ExecNum; i++ {
			sum += i
		}
	}
	Info("Exec result:", sum)
	LogStageEnd("Exec")
	execTime := time.Since(start)
	replica.stat.execTimeSum += execTime
	replica.stat.execTimeCnt++

	Info("Exec time:", execTime)
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
		Panic("json.Marshal(signMsg), err: %v", err)
	}
	for _, node := range KConfig.Id2Node {
		if node.id == replica.node.id {
			continue
		}
		node.connMgr.sendChan <- signMsgBytes
	}
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
	jsonMsg, err := json.Marshal(req)
	if err != nil {
		Warn("err: %v", err)
	}
	Info("req sz =", float64(len(jsonMsg))/MBSize)

	Info("# boost num =", KConfig.ReqNum)
	Info("# boostSeqChan len:", len(replica.boostChan))
	//time.Sleep(time.Duration(boostDelay) * time.Millisecond)

	replica.boostChan <- 0

	LogStageStart(fmt.Sprintf("Batch %d", replica.batchSeq))
	prefix := replica.node.id * 10000

	time.Sleep(time.Millisecond * time.Duration(KConfig.StartDelay))
	start := time.Now()
	for batchSeq := range replica.boostChan {
		if batchSeq == int64(KConfig.ReqNum) {
			break
		}
		Info("# batchSeq:", batchSeq)

		req.Seq = prefix + batchSeq
		req.Timestamp = time.Now().UnixNano()
		msgCert := replica.getMsgCert(req.Seq)
		msgCert.Req = req
		msgCert.SendPrePrepare = WaitSend
		replica.sendPrePrepare(msgCert)
		//signMsg := pbft.signMsg(req)
		//// pbft.RecvChan.RequestMsgChan <- reqMsg
		//recvChan <- signMsg
	}
	Info("\033[32m[req]\033[0m req finish, KConfig.ReqNum:", KConfig.ReqNum, "spend time:", time.Since(start))

	time.Sleep(time.Second * 60)
	for _, node := range KConfig.Id2Node {
		node.connMgr.closeTcpConn()
	}
	Info("===> Exit")
	os.Exit(1)
}

func (replica *Replica) handleProposal(msg *Message) {
	msgCert := replica.getMsgCert(msg.Seq)

	replica.sendRequest(msgCert)
}

func (replica *Replica) sendRequest(msgCert *MsgCert) {
	//req := &Message{
	//	MsgType:   MtRequest,
	//	Seq:       msgCert.Seq,
	//	NodeId:    pbft.node.id,
	//	Timestamp: time.Now().UnixNano(),
	//	Txs:       &BatchTx{},
	//}
}
