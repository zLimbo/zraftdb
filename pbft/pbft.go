package pbft

import (
	"encoding/json"
	"fmt"
	"log"
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

type PbftStat struct {
	requestNum, prePrepareNum, prepareNum, commitNum, replyNum int64
	prePrepareTime, prepareTime, commitTime                    int64
	batchTimeOkNum                                             int64
	time0, time1, time2, time3, time4                          time.Duration
	time3pcSum, time2pcSum                                     int64
	count3pc, count2pc                                         int64
	execTimeSum, execTimeCnt                                   time.Duration
	signTimeSum, signTimeCnt                                   time.Duration
	verifyTimeSum, verifyTimeCnt                               time.Duration
	times                                                      []int64
}

func NewPbftStat() *PbftStat {
	stat := &PbftStat{
		times: make([]int64, 0),
	}
	return stat
}

type Pbft struct {
	node        *Node
	stat        *PbftStat
	msgCertPool map[int64]*MsgCert

	boostChan chan int64
	batchPool map[int64]*Batch
	batchSeq  int64
	curBatch  *Batch
}

func NewPbft(id int64) *Pbft {
	pbft := &Pbft{
		node:        GetNode(id),
		stat:        NewPbftStat(),
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

func (pbft *Pbft) Start(reqNum, boostDelay int) {

	go pbft.connect()
	go pbft.listen()
	go pbft.keep()
	go pbft.handleMsg()
	//go pbft.status()
	//go pbft.flowStatistics()

	if GetIndex(pbft.node.ip) < BoostNum {
		go pbft.boostReq(reqNum, boostDelay)
	}

	select {}
}

func (pbft *Pbft) handleMsg() {
	for signMsg := range recvChan {

		node := GetNode(signMsg.Msg.NodeId)
		start := time.Now()
		if !VerifySignMsg(signMsg, node.pubKey) {
			log.Panic("#### verify failed!")
		}
		verifyTime := time.Since(start)
		if signMsg.Msg.MsgType == MtPrePrepare {
			pbft.stat.verifyTimeSum += verifyTime
			pbft.stat.verifyTimeCnt++
		}

		msg := signMsg.Msg
		log.Println("+ handle start + batch seq:", pbft.batchSeq, "recvChan size:", len(recvChan))
		//log.Println("recv msg{", msg.MsgType, msg.Seq, msg.NodeId, msg.Timestamp, "}")
		switch msg.MsgType {
		case MtProposal:
			pbft.handleProposal(msg)
		case MtRequest:
			pbft.handleRequest(msg)
		case MtPrePrepare:
			pbft.handlePrePrepare(msg)
		case MtPrepare:
			pbft.handlePrepare(msg)
		case MtCommit:
			pbft.handleCommit(msg)
		}

		log.Printf("[req=%d, pre-prepare=%d, prepare=%d, commit=%d, reply=%d, recvchan_size=%d, noConnCnt=%d]\n",
			pbft.stat.requestNum,
			pbft.stat.prePrepareNum,
			pbft.stat.prepareNum,
			pbft.stat.commitNum,
			pbft.stat.replyNum,
			len(recvChan),
			noConnCnt)
		log.Println("[recv pre-prepare num:", pbft.curBatch.prePrepareMsgNum, "]")
	}
}

func (pbft *Pbft) getMsgCert(seq int64) *MsgCert {
	msgCert, ok := pbft.msgCertPool[seq]
	if !ok {
		msgCert = NewMsgCert()
		pbft.msgCertPool[seq] = msgCert
		msgCert.Seq = seq
	}
	return msgCert
}

func (pbft *Pbft) handleRequest(msg *Message) {
	log.Println("<node handleRequestMsg> msg seq=", msg.Seq, "tx num", len(msg.Txs))
	msgCert := pbft.getMsgCert(msg.Seq)
	if msgCert.Req != nil {
		log.Printf("The request(seq=%d) has been accepted!\n", msg.Seq)
		return
	}

	msgCert.Seq = msg.Seq
	msgCert.Req = msg
	msgCert.SendPrePrepare = WaitSend

	msgCert.Time = time.Now().UnixNano()
	msgCert.PrePrepareTime = time.Now().UnixNano()

	pbft.curBatch.times[1] = time.Now()

	LogStageStart(fmt.Sprintf("seq=%d pre-prepare", msgCert.Seq))

	pbft.sendPrePrepare(msgCert)
}

func (pbft *Pbft) handlePrePrepare(msg *Message) {
	log.Println("<node handlePrePrepareMsg> msg seq=", msg.Seq, "tx num", len(msg.Txs))
	msgCert := pbft.getMsgCert(msg.Seq)
	if msgCert.PrePrepare != nil {
		log.Printf("The pre-prepare(seq=%d) has been accepted!\n", msg.Seq)
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

	pbft.sendPrepare(msgCert)
}

func (pbft *Pbft) handlePrepare(msg *Message) {
	msgCert := pbft.getMsgCert(msg.Seq)
	if msgCert.SendCommit == HasSend {
		return
	}
	log.Printf("<node handlePrepareMsg> msg seq=%d nodeId=%d\n", msg.Seq, msg.NodeId)
	log.Println("\t\033[32mtime:\033[0m", time.Duration(time.Now().UnixNano()-msgCert.PrePrepareTime))
	pbft.recvPrepareMsg(msgCert, msg)
	pbft.maybeSendCommit(msgCert)
}

func (pbft *Pbft) handleCommit(msg *Message) {
	msgCert := pbft.getMsgCert(msg.Seq)
	if msgCert.SendReply == HasSend {
		return
	}
	log.Printf("<node handleCommitMsg> msg seq=%d nodeId=%d\n", msg.Seq, msg.NodeId)
	pbft.recvCommitMsg(msgCert, msg)
	pbft.maybeSendReply(msgCert)
}

func (pbft *Pbft) recvPrepareMsg(msgCert *MsgCert, msg *Message) {
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
	log.Printf("same prepare msg count=%d\n", count)
	if count >= 2*f {
		msgCert.SendCommit = WaitSend
	}
}

func (pbft *Pbft) recvCommitMsg(msgCert *MsgCert, msg *Message) {
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
	log.Printf("same commit msg count=%d\n", count)
	if count >= 2*f+1 {
		msgCert.SendReply = WaitSend
	}
}

func (pbft *Pbft) sendPrePrepare(msgCert *MsgCert) {
	prePrepareMsg := &Message{
		MsgType:   MtPrePrepare,
		Seq:       msgCert.Seq,
		NodeId:    pbft.node.id,
		Timestamp: time.Now().UnixNano(),
		Req:       msgCert.Req,
	}
	signMsg := pbft.signMsg(prePrepareMsg)
	pbft.broadcast(signMsg, msgCert)
	msgCert.SendPrePrepare = HasSend
	log.Println("[pre-prepare] msg has been sent.")

	msgCert.PrePrepareTime = time.Now().UnixNano() - msgCert.PrePrepareTime
	LogStageEnd(fmt.Sprintf("seq=%d pre-prepare", msgCert.Seq))

	msgCert.PrepareTime = time.Now().UnixNano()
	msgCert.SendPrepare = HasSend
	LogStageStart(fmt.Sprintf("seq=%d prepare", msgCert.Seq))
	pbft.maybeSendCommit(msgCert)
}

func (pbft *Pbft) sendPrepare(msgCert *MsgCert) {
	prepareMsg := &Message{
		MsgType:   MtPrepare,
		Seq:       msgCert.Seq,
		NodeId:    pbft.node.id,
		Timestamp: time.Now().UnixNano(),
	}
	pbft.recvPrepareMsg(msgCert, prepareMsg)
	signMsg := pbft.signMsg(prepareMsg)
	log.Println("<broadcast> prepare")
	pbft.broadcast(signMsg, msgCert)
	msgCert.SendPrepare = HasSend
	pbft.stat.prePrepareNum++

	//pbft.curBatch.counts[2]++
	//if pbft.curBatch.counts[2] == 2*f+1 {
	//	pbft.curBatch.times[2] = time.Now()
	//}

	log.Println("[prepare] msg has been sent.")
	pbft.maybeSendCommit(msgCert)
}

func (pbft *Pbft) maybeSendCommit(msgCert *MsgCert) {
	if msgCert.SendPrepare != HasSend || msgCert.SendCommit != WaitSend {
		return
	}
	pbft.stat.prepareNum++

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
		NodeId:    pbft.node.id,
		Timestamp: time.Now().UnixNano(),
	}

	pbft.recvCommitMsg(msgCert, commitMsg)
	signMsg := pbft.signMsg(commitMsg)
	pbft.broadcast(signMsg, msgCert)
	msgCert.SendCommit = HasSend
	log.Println("[commit] msg has been sent.")
	pbft.maybeSendReply(msgCert)
}

func (pbft *Pbft) maybeSendReply(msgCert *MsgCert) {
	if msgCert.SendCommit != HasSend || msgCert.SendReply != WaitSend {
		return
	}

	msgCert.Time = time.Now().UnixNano() - msgCert.Time
	msgCert.CommitTime = time.Now().UnixNano() - msgCert.CommitTime
	pbft.stat.commitNum++
	LogStageEnd(fmt.Sprintf("seq=%d commit", msgCert.Seq))

	LogStageStart(fmt.Sprintf("seq=%d reply", msgCert.Seq))
	replyMsg := &Message{
		MsgType:   MtReply,
		Seq:       msgCert.Seq,
		NodeId:    pbft.node.id,
		Timestamp: time.Now().UnixNano(),
	}
	signMsg := pbft.signMsg(replyMsg)
	ClientNode.netMgr.sendChan <- signMsg
	// go PostJson(ClientUrl+"/getReply", jsonMsg)
	msgCert.SendReply = HasSend

	log.Printf("[reply] msg has been sent, seq=%d\n", msgCert.Seq)
	LogStageEnd(fmt.Sprintf("seq=%d reply reply_count=%d", msgCert.Seq, pbft.stat.replyNum))
	pbft.stat.replyNum++

	pbft.finalize(msgCert)
}

func (pbft *Pbft) finalize(msgCert *MsgCert) {
	// pbft.showTime(msgCert)

	pbft.curBatch.counts[4]++
	if pbft.curBatch.counts[4] == BoostNum {
		pbft.curBatch.times[4] = time.Now()
		LogStageEnd(fmt.Sprintf("Batch %d", pbft.batchSeq))

		pbft.showBatchTime()
		//pbft.exec(BoostNum)
		pbft.curBatch = &Batch{}
		pbft.curBatch.times[0] = time.Now()
		pbft.batchSeq += 1
		pbft.batchPool[pbft.batchSeq] = pbft.curBatch
		pbft.boostChan <- pbft.batchSeq

		LogStageStart(fmt.Sprintf("Batch %d", pbft.batchSeq))
	}

	pbft.showTime(msgCert)
	pbft.clearCert(msgCert)
}

func (pbft *Pbft) exec(num int) {
	LogStageStart("Exec")
	start := time.Now()
	sum := int64(0)
	for j := 0; j < num; j++ {
		for i := int64(0); i < ExecNum; i++ {
			sum += i
		}
	}
	log.Println("Exec result:", sum)
	LogStageEnd("Exec")
	execTime := time.Since(start)
	pbft.stat.execTimeSum += execTime
	pbft.stat.execTimeCnt++

	log.Println("Exec time:", execTime)
}

func (pbft *Pbft) clearCert(msgCert *MsgCert) {
	msgCert.Req = nil
	msgCert.PrePrepare = nil
	msgCert.Prepares = nil
	msgCert.Commits = nil
}

func (pbft *Pbft) broadcast(signMsg *SignMessage, msgCert *MsgCert) {

	for _, node := range NodeTable {
		if node.id == pbft.node.id {
			continue
		}
		node.netMgr.sendChan <- signMsg
	}
}

func (pbft *Pbft) boostReq(reqNum, boostDelay int) {

	if reqNum <= 0 {
		return
	}

	req := &Message{
		MsgType:   MtRequest,
		Seq:       1,
		NodeId:    pbft.node.id,
		Timestamp: time.Now().UnixNano(),
		Txs:       &BatchTx{},
	}
	SignRequest(req, pbft.node.priKey)
	jsonMsg, err := json.Marshal(req)
	if err != nil {
		log.Println(err)
	}
	log.Println("req sz =", float64(len(jsonMsg))/MBSize)

	log.Println("# boost num =", reqNum)
	log.Println("# boostSeqChan len:", len(pbft.boostChan))
	//time.Sleep(time.Duration(boostDelay) * time.Millisecond)

	pbft.boostChan <- 0

	LogStageStart(fmt.Sprintf("Batch %d", pbft.batchSeq))
	prefix := pbft.node.id * 10000

	time.Sleep(time.Millisecond * ClientDelay)
	start := time.Now()
	for batchSeq := range pbft.boostChan {
		if batchSeq == int64(reqNum) {
			break
		}
		log.Println("# batchSeq:", batchSeq)

		req.Seq = prefix + batchSeq
		req.Timestamp = time.Now().UnixNano()
		msgCert := pbft.getMsgCert(req.Seq)
		msgCert.Req = req
		msgCert.SendPrePrepare = WaitSend
		pbft.sendPrePrepare(msgCert)
		//signMsg := pbft.signMsg(req)
		//// pbft.RecvChan.RequestMsgChan <- reqMsg
		//recvChan <- signMsg
	}
	log.Println("\033[32m[req]\033[0m req finish, reqNum:", reqNum, "spend time:", time.Since(start))

	time.Sleep(time.Second * 60)
	for _, node := range NodeTable {
		node.netMgr.closeTcpConn()
	}
	log.Println("===> Exit")
	os.Exit(1)
}

func (pbft *Pbft) handleProposal(msg *Message) {
	msgCert := pbft.getMsgCert(msg.Seq)

	pbft.sendRequest(msgCert)
}

func (pbft *Pbft) sendRequest(msgCert *MsgCert) {
	//req := &Message{
	//	MsgType:   MtRequest,
	//	Seq:       msgCert.Seq,
	//	NodeId:    pbft.node.id,
	//	Timestamp: time.Now().UnixNano(),
	//	Txs:       &BatchTx{},
	//}
}
