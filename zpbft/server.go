package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	node      *Node
	seqCh     chan int64
	logs      []*Log
	seq2cert  map[int64]*LogCert
	id2srvCli map[int64]*rpc.Client
	id2cliCli map[int64]*rpc.Client
	mu        sync.Mutex
	txPoolMu        sync.Mutex
	eachInstanceViewLocallyMutex        sync.Mutex
	localNodeSendingTxsMutex        sync.Mutex
	tpsMutex sync.Mutex
	viewEndTimeMu sync.Mutex
	seqInc    int64
	view      int64
	eachInstanceViewLocally map[int64]int64
	currentView int64
	viewCommittedInstance map[int64]int64 //
	localViewCommitted LocalView
	randomDelay int64
	startTime time.Time
	endTime time.Time
	proposers []int64
	isProposer bool
	txPool int64
	localNodeSendingTxs int64
	currentConfirmedTx int64
	delay int64
	cumulative int64
	tps []float64
	roundEndTime []time.Time
	latencyPerRound []float64
	viewEndTime []time.Time
	viewStartTime []time.Time
	latencyPerView []float64
	delayPerView []int64
	sysStartToViewStart []float64
	sysStartToViewEnd []float64
	rotateOrNot bool
	randomDelayOrNot bool

	//for PRaft
	currentTerm int64
	currentBlockIndex int64
	duplicateMu sync.Mutex
	prepareMu sync.Mutex
	height2blockLogMu sync.Mutex
	localDuplicatedMu sync.Mutex
	localDuplicatedReqs []*duplicatedReqUnit
	height2blockLog map[int64]*BlockLog
	localCommittedTxNum int64
	throughput float64
	txPoolTime time.Time
	txPoolBatches []*txPoolUnit
	dupTime []int64
}

func (s *Server) pushTxToPool() {
	// 后8位为节点id
	for{
		//randomDelay, _ := rand.Int(rand.Reader, big.NewInt(int64(KConfig.Delay)))
		//randomDelay2 := randomDelay.Int64()
		//time.Sleep(time.Duration(randomDelay2) * time.Millisecond)
		//Debug("random duration = %d", randomDelay2)
		s.txPoolMu.Lock()
		s.txPool += int64(KConfig.Load/KConfig.ProposerNum)
		s.cumulative += int64(KConfig.Load/KConfig.ProposerNum)

		//time for txPool
		newTxsUnit := &txPoolUnit{
			txNum: int64(KConfig.Load / KConfig.ProposerNum),
			arrivalTime: time.Now(),
			completed: false,
		}
		s.txPoolBatches = append(s.txPoolBatches, newTxsUnit)

		s.txPoolMu.Unlock()
		time.Sleep(1000*time.Millisecond)
	}
}

func (s *Server)PrimaryReceiveBackRpc(args *DuplicateConfirmArgs2, returnArgs *TreeBCBackReplyArgs) error {

	for i := 0; i < len(args.Args); i++ {
		cert := s.getCertOrNew(args.Args[i].Msg.Seq)
		cert.pushDuplicateConfirm(args.Args[i])

		s.duplicateMu.Lock()
		if cert.stage == InitialStage{
			s.verifyBallot(cert)
		}
		s.duplicateMu.Unlock()
		returnArgs.Ok = 1
	}
	//Debug("receive broadcast back from %d", args.Msg.NodeId)

	return nil
}
//只有根广播者才会调用
func (s *Server)treeDuplicate(seq int64){

	req, digest, duplicator, _ := s.getCertOrNew(seq).get()
	tree := makeTree(s.id2srvCli, 2, s.node.id)
	treeBCMsg := &TreeBroadcastMsg{
		Seq: seq,
		Digest: digest,
		NodeId: s.node.id,
		DuplicatorNodeId: duplicator,
		TxNum: int64(req.TxNum),
		Tree: tree,
	}
	digest = Sha256Digest(treeBCMsg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	treeBCArgs := TreeBroadcastArgs{
		TreeBCMsgs: treeBCMsg,
		Digest: digest,
		Sign: sign,
		ReqArgs: req,
	}

	idArray := traverseTree(tree, s.node.id)
	//for i := 0; i < len(idArray); i++ {
	//	Debug("idArray[%d] = %d", i, idArray[i])
	//}
	//var duplicateConfirmArgs2Lock sync.Mutex
    //var duplicateConfirmArgs2 DuplicateConfirmArgs2

	for i := 0; i < len(idArray); i++ {
		id := idArray[i]
		srvCli := s.id2srvCli[id]
		go func() { // 异步发送
			var returnArgs DuplicateConfirmArgs
			//Debug("Broadcast to %d", id)
			err := srvCli.Call("Server.TreeDuplicateRpc", treeBCArgs, &returnArgs)
			if err != nil {
				Error("Server.TreeDuplicateRpc %d error: %v", id, err)
			}
			if &returnArgs == nil{
				Error("Calling TreeDuplicateRpc method error")
			}
			cert := s.getCertOrNew(returnArgs.Msg.Seq)
			cert.pushDuplicateConfirm(&returnArgs)

			s.duplicateMu.Lock()
			if cert.stage == InitialStage{
				s.verifyBallot(cert)
			}
			s.duplicateMu.Unlock()
		}()
	}
	//if len(duplicateConfirmArgs2.Args) == len(idArray) {
	//
	//}
}

func (s *Server) TreeDuplicateRpc(args *TreeBroadcastArgs, returnArgs *DuplicateConfirmArgs) error {
	idNodes := []int64{
		14812901,
		14812902,
		14812903,
		14812904,
		14812905,
		14814701,
		14814702,
		14814703,
		14814704,
		14814705,
		14821701,
		14821702,
		14821703,
		14821704,
		14821705,
	}
	for i := 0; i < len(idNodes);i++{
		if s.node.id == idNodes[i]{
			time.Sleep(2*time.Second)
		}
	}
	msg := args.TreeBCMsgs
	//Debug("Receive tree broadcast msg from %d", msg.NodeId)
	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("treeDuplicateMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}
	nextNodes := traverseTree(msg.Tree, s.node.id)

	duplicateConfirmMsg := &DuplicateConfirmMsg{
		Seq:    msg.Seq,
		Digest: digest,
		NodeId: s.node.id,
		DuplicatorNodeId: msg.Tree.Id,
	}
	digest = Sha256Digest(duplicateConfirmMsg)
	sign := RsaSignWithSha256(digest, s.node.priKey)

	//duplicateConfirmArgs := DuplicateConfirmArgs{
	//	Msg: duplicateConfirmMsg,
	//	Sign: sign,
	//}
	//go func(){
	//	srvCli := s.id2srvCli[msg.Tree.Id]
	//	var returnArgs TreeBCBackReplyArgs
	//	Debug("Send back to primary %d", msg.Tree.Id)
	//	err := srvCli.Call("Server.PrimaryReceiveBackRpc", duplicateConfirmArgs, &returnArgs)
	//	if err != nil {
	//		Error("Server.PrimaryReceiveBackRpc %d error: %v", msg.Tree.Id, err)
	//	}
	//	if &returnArgs == nil{
	//		Error("Calling Server.PrimaryReceiveBackRpc method error")
	//	}
	//}()
	var duplicateConfirmArgs2Lock sync.Mutex
	var duplicateConfirmArgs2 DuplicateConfirmArgs2

	//idArray := traverseTree(args.TreeBCMsgs.Tree, s.node.id)
	//for i := 0; i < len(idArray); i++ {
	//	Debug("idArray[%d] = %d", i, idArray[i])
	//}
	ch := make(chan int)
	if len(nextNodes) > 0 {
		for i := 0; i < len(nextNodes); i++ {
			i := i
			go func() {
				var returnArgs DuplicateConfirmArgs
				var newArgs TreeBroadcastArgs
				//不可直接将args.TreeBCMsgs直接赋值给newArgs.TreeBCMsgs，由于TreeBCMsgs是指针，后面在修改nodeId时，会和其他线程冲突
				treeBCMsg := &TreeBroadcastMsg {
					TxNum: args.TreeBCMsgs.TxNum,
					Digest: args.TreeBCMsgs.Digest,
					Tree: args.TreeBCMsgs.Tree,
					Seq: args.TreeBCMsgs.Seq,
					DuplicatorNodeId: args.TreeBCMsgs.DuplicatorNodeId,
					NodeId: s.node.id,

				}
				newArgs.ReqArgs = args.ReqArgs
				newArgs.TreeBCMsgs = treeBCMsg
				newArgs.Digest = Sha256Digest(newArgs.TreeBCMsgs)
				newArgs.Sign = RsaSignWithSha256(newArgs.Digest, s.node.priKey)
				if nextNodes[i] == s.node.id {
					return
				}
				srvCli := s.id2srvCli[nextNodes[i]]
				//Debug("Preparing send to %d", nextNodes[i])
				//Debug("Continue to broadcast to %d", nextNodes[i])

				err := srvCli.Call("Server.TreeDuplicateRpc", newArgs, &returnArgs)
				if err != nil {
					Error("Server.TreeDuplicateRpc %d error: %v", nextNodes[i], err)
				}
				if &returnArgs == nil{
					Error("Calling TreeDuplicateRpc method error")
				}

				duplicateConfirmArgs2Lock.Lock()
				duplicateConfirmArgs2.Args = append(duplicateConfirmArgs2.Args, &returnArgs)
				if len(duplicateConfirmArgs2.Args) == len(nextNodes){
					ch <- 1
				}
				duplicateConfirmArgs2Lock.Unlock()

			}()
		}
	}
	go func(){
		 <- ch
		srvCli := s.id2srvCli[msg.Tree.Id]
		var returnArgs TreeBCBackReplyArgs
		//Debug("Send back to primary %d", msg.Tree.Id)
		err := srvCli.Call("Server.PrimaryReceiveBackRpc", duplicateConfirmArgs2, &returnArgs)
		if err != nil {
			Error("Server.PrimaryReceiveBackRpc %d error: %v", msg.Tree.Id, err)
		}
		if &returnArgs == nil{
			Error("Calling Server.PrimaryReceiveBackRpc method error")
		}
	}()



	//returnMsg := &DuplicateConfirmArgs{
	//	Msg: duplicateConfirmMsg,
	//	Sign: sign,
	//}
	//digest = Sha256Digest(returnMsg)
	sign = RsaSignWithSha256(digest, s.node.priKey)
	returnArgs.Msg = duplicateConfirmMsg
	//returnArgs.Digest = digest
	returnArgs.Sign = sign
	return nil
}


func (s *Server) duplicate(seq int64){
	//广播线程从seq2cert中获取请求参数，并构造prepare消息

	//构造duplicate消息------------------------
	//startTime := time.Now()
	req, digest, duplicator, _ := s.getCertOrNew(seq).get()
	msg := &DuplicateMsg{
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
		DuplicatorNodeId: duplicator,
		TxNum: int64(req.TxNum),
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数
	args := &DuplicateArgs{
		Msg:     msg,
		Sign:    sign,
		ReqArgs: req,
	}
	//--------------------------------------

	// 发送duplicate消息
	//Debug("node[%d] Start duplicating request %s(hash value)\n", s.node.id, digest)
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var returnArgs DuplicateConfirmArgs
			err := srvCli1.Call("Server.DuplicateRpc", args, &returnArgs)
			if err != nil {
				Error("Server.DuplicateRpc %d error: %v", id1, err)
			}
			if &returnArgs == nil{
				Error("Calling DuplicateRpc method error")
			}
			cert := s.getCertOrNew(msg.Seq)
			cert.pushDuplicateConfirm(&returnArgs)
			s.duplicateMu.Lock()
			if cert.stage == InitialStage{
				s.verifyBallot(cert)
			}
			s.duplicateMu.Unlock()
		}()
	}
	//endTime := time.Now()
	//Debug("Duplicating time duration = %d ms", endTime.Sub(startTime).Milliseconds())
}

func (s *Server) DuplicateRpc(args *DuplicateArgs, returnArgs *DuplicateConfirmArgs) error {
	idNodes := []int64{
		14812901,
		14812902,
		14812903,
		14812904,
		14812905,
		14814701,
		14814702,
		14814703,
		14814704,
		14814705,
		14821701,
		14821702,
		14821703,
		14821704,
		14821705,
	}
	for i := 0; i < len(idNodes);i++{
		if s.node.id == idNodes[i]{
			time.Sleep(2*time.Second)
		}
	}
	msg := args.Msg
	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("DuplicateMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}
	//Debug("Received duplicate message from node[%d]", args.Msg.DuplicatorNodeId)
	reqArgs := args.ReqArgs
	//node = GetNode(reqArgs.Req.ClientId)
	node = GetNode(msg.NodeId)
	digest = Sha256Digest(reqArgs.Req)
	if !SliceEqual(digest, msg.Digest) {
		Warn("DuplicateMsg error, req.digest != msg.Digest")
		return nil
	}

	cert := s.getCertOrNew(msg.Seq)
	if cert.committed {
		return nil
	}
	//cert.set(reqArgs, digest, msg.View,msg.PrimaryNodeId)
	cert.set(nil, digest, msg.logIndex, msg.DuplicatorNodeId)
	_, digest, duplicator, logIndex:= s.getCertOrNew(msg.Seq).get()

	returnMsg := &DuplicateConfirmMsg{
		Seq:    cert.seq,
		Digest: digest,
		NodeId: s.node.id,
		DuplicatorNodeId: duplicator,
		LogIndex: logIndex,
	}
	digest = Sha256Digest(returnMsg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	returnArgs.Msg = returnMsg
	returnArgs.Sign = sign
	return nil
}

func (s *Server) delayReset(){
	if s.randomDelayOrNot {
		Debug("random delay set==============")
		randomDelay, _ := rand.Int(rand.Reader, big.NewInt(int64(KConfig.Delay)))
		s.randomDelay = randomDelay.Int64()
	}else{
		Debug("random delay not set==============")
		s.randomDelay = int64(KConfig.Delay)
	}

}
func (s *Server) rotateProposers(viewNum int, proposersNum int){
	index := viewNum % len(KConfig.PeerIps)
	for i := 0; i < proposersNum;i++{
		s.proposers[i] = KConfig.PeerIds[(index + i) % len(KConfig.PeerIps)]
	}
}

func (s *Server) assignSeq() int64 {
	// 后8位为节点id
	return atomic.AddInt64(&s.seqInc, 1e10)
}

func (s *Server) getCertOrNew(seq int64) *LogCert {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, ok := s.seq2cert[seq]
	if !ok {
		cert = &LogCert{
			seq:      seq,
			prepares: make(map[int64]*PrepareArgs),
			prepareConfirms: make(map[int64]*PrepareConfirmArgs),
			duplicateConfirms: make(map[int64]*DuplicateConfirmArgs),
			prepareQ: make([]*PrepareArgs, 0),
			prepareConfirmQ: make([]*PrepareConfirmArgs, 0),
		}
		s.seq2cert[seq] = cert
	}
	return cert
}

func (s *Server) RequestRpc(args *RequestArgs, reply *RequestReply) error {
	// 放入请求队列直接返回，后续异步通知客户端

	Debug("RequestRpc, from: %d", args.Req.ClientId)
	//构造请求
	req := &RequestMsg{
		Operator:  make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
		Timestamp: time.Now().UnixNano(),
		ClientId:  args.Req.ClientId,
	}
	node := GetNode(args.Req.ClientId)
	digest := Sha256Digest(req)
	sign := RsaSignWithSha256(digest, node.priKey)

	args = &RequestArgs{
		Req:  req,
		Sign: sign,
	}
	// 验证RequestMsg
	// node := GetNode(args.Req.ClientId)
	// digest := Sha256Digest(args.Req)
	// ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	// if !ok {
	// 	Warn("RequestMsg verify error, from: %d", args.Req.ClientId)
	// 	reply.Ok = false
	// 	return nil
	// }

	// leader 分配seq
	seq := s.assignSeq()
	//主节点新建logCert，设置参数，并存储在seq2cert中
	s.getCertOrNew(seq).set(args, digest, s.view,s.node.id)
	//向共识线程发送开始共识信号
	s.seqCh <- seq

	// 返回信息
	reply.Seq = seq
	reply.Ok = true

	return nil
}

func (s *Server) controlSending(){
	startTime := time.Now()
	if s.isProposer {
		for{
			time.Sleep(100*time.Millisecond)
			s.localDuplicatedMu.Lock()
			if len(s.localDuplicatedReqs) != 0 {
				Debug("System current duplicated req num = %d", len(s.localDuplicatedReqs))
				s.localDuplicatedMu.Unlock()
				s.Sending()
				Debug("System current committed tx num = %d", s.localCommittedTxNum)
				endTime := time.Now()
				Debug("Time duration is %f, System throughput is %f", endTime.Sub(startTime).Seconds(), float64(s.localCommittedTxNum)/endTime.Sub(startTime).Seconds())
                if float64(s.localCommittedTxNum)/endTime.Sub(startTime).Seconds() > s.throughput {
                	s.throughput = float64(s.localCommittedTxNum)/endTime.Sub(startTime).Seconds()
				}
				Debug("Maximum throughput is %f", s.throughput)
			} else {
				s.localDuplicatedMu.Unlock()
				time.Sleep(1000*time.Millisecond)
				s.localDuplicatedMu.Lock()
				Debug("System current duplicated req num = %d", len(s.localDuplicatedReqs))
				s.localDuplicatedMu.Unlock()
				s.Sending()
				Debug("System current committed tx num = %d", s.localCommittedTxNum)
				endTime := time.Now()
				Debug("Time duration is %f, System throughput is %f", endTime.Sub(startTime).Seconds(), float64(s.localCommittedTxNum)/endTime.Sub(startTime).Seconds())
				if float64(s.localCommittedTxNum)/endTime.Sub(startTime).Seconds() > s.throughput {
					s.throughput = float64(s.localCommittedTxNum)/endTime.Sub(startTime).Seconds()
				}
				Debug("Maximum throughput is %f", s.throughput)
			}
		}
	}
}
//******    rpc发送的结构体中的元素首字母必须大写
func (s *Server) Sending(){
	s.currentBlockIndex++
	//构造新的区块
	block := &Block{
		BlockIndex: s.currentBlockIndex,
		DuplicatedReqs: nil,
		Committed: false,
		TxNum: 0,
	}

	//将本地可靠广播的请求放到区块中，并将本地可靠广播的请求清空
	s.localDuplicatedMu.Lock()
	Debug("s.localDuplicatedReq size = %d (before clear)\n", len(s.localDuplicatedReqs))
	for i := 0; i < len(s.localDuplicatedReqs); i++{
		block.DuplicatedReqs = append(block.DuplicatedReqs, s.localDuplicatedReqs[i])
		block.TxNum = block.TxNum + s.localDuplicatedReqs[i].TxNum
	}
	Debug("block tx number = %d", block.TxNum)
	//block.duplicatedReqsJson,_ = json.Marshal(duplicatedReqArray)
	s.localDuplicatedReqs = nil
	Debug("s.localDuplicatedReq size = %d (after clear)\n", len(s.localDuplicatedReqs))
	s.localDuplicatedMu.Unlock()

	//构造发送给其他的节点的消息，包括当前区块高度和前一个区块的commit信号
	msg := &SendingMsg{
		CommitBlockIndex: s.currentBlockIndex - 1,
		//CommitBlockTxNum: s.height2blockLog[s.currentBlockIndex-1].txNum,
		PrimaryNodeId: s.node.id,
		Block: block,
	}
	digest := Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	args := &SendingArgs{
		Msg: msg,
		Digest: digest,
		Sign: sign,
	}
	//将新产生的区块添加到本地日志中，可通过区块高度访问区块
    s.localDuplicatedMu.Lock()
	newBlockLog := &BlockLog{
		blockIndex: s.currentBlockIndex,
		duplicatedReqs: s.localDuplicatedReqs,
		txNum: block.TxNum,
		prepared: false,
		committed: false,
	}
	s.localCommittedTxNum += block.TxNum
	s.localDuplicatedMu.Unlock()
	s.height2blockLogMu.Lock()
	s.height2blockLog[newBlockLog.blockIndex] = newBlockLog
	s.height2blockLogMu.Unlock()


    //将构造的消息发送给其他节点
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var returnArgs SendingReturnArgs
			err := srvCli1.Call("Server.Receiving", args, &returnArgs)
			if err != nil {
				Error("Server.Receiving %d error: %v", id1, err)
			}
			if &returnArgs == nil{
				Error("Calling Receiving method error")
			}
			//cert := s.getCertOrNew(msg.Seq)
			//构造新区块，并确认前一个区块
			s.height2blockLogMu.Lock()
            prepareBlockLog, ok := s.height2blockLog[returnArgs.Msg.PrepareBlockIndex]
            if !ok{
				s.height2blockLogMu.Unlock()
            	return
			}
			commitBlockLog, ok := s.height2blockLog[returnArgs.Msg.CommittedBlockIndex]
			if !ok{
				s.height2blockLogMu.Unlock()
				return
			}
			s.height2blockLogMu.Unlock()
			prepareBlockLog.blockLogMutex.Lock()
			commitBlockLog.blockLogMutex.Lock()
			prepareBlockLog.prepareConfirmNodes = append(prepareBlockLog.prepareConfirmNodes, returnArgs.Msg.NodeId)
			commitBlockLog.commitConfirmNodes = append(commitBlockLog.commitConfirmNodes, returnArgs.Msg.NodeId)
			prepareBlockLog.check()
			commitBlockLog.check()
			commitBlockLog.blockLogMutex.Unlock()
			prepareBlockLog.blockLogMutex.Unlock()

			if len(returnArgs.Msg.NewDuplicatedReqs) > 0 {
				s.localDuplicatedMu.Lock()
				//Debug("current duplicated pool is %d, append duplicated req from %d, duplicated num = %d", len(s.localDuplicatedReqs), returnArgs.Msg.NodeId, len(returnArgs.Msg.NewDuplicatedReqs))
				for i := 0; i < len(returnArgs.Msg.NewDuplicatedReqs); i++{
					s.localDuplicatedReqs = append(s.localDuplicatedReqs, returnArgs.Msg.NewDuplicatedReqs[i])
					//Debug("received report unit tx num = %d", returnArgs.Msg.NewDuplicatedReqs[i].TxNum)
				}
				//Debug("Has added %d's duplicated reqs, current duplicated pool is %d", returnArgs.Msg.NodeId, len(s.localDuplicatedReqs))
				s.localDuplicatedMu.Unlock()
			}
		}()
	}
}
func (s *Server) Receiving(args *SendingArgs, returnArgs *SendingReturnArgs) error{
	msg := args.Msg
	//Debug("block req size = %d", len(msg.Block.DuplicatedReqs))

	node := GetNode(msg.PrimaryNodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("SendingMsg verify error, block height: %d, from: %d", msg.Block.BlockIndex, msg.PrimaryNodeId)
		return nil
	}
	s.height2blockLogMu.Lock()
	prepareBlockLog, ok := s.height2blockLog[msg.Block.BlockIndex]
	s.height2blockLogMu.Unlock()
	if !ok{
		prepareBlockLog := &BlockLog{
			blockIndex: msg.Block.BlockIndex,
			prepared: true,
			committed: false,
			duplicatedReqs: msg.Block.DuplicatedReqs,
			primaryNodeId: msg.PrimaryNodeId,
		}
		s.height2blockLogMu.Lock()
		s.height2blockLog[msg.Block.BlockIndex] = prepareBlockLog
		s.height2blockLogMu.Unlock()
		//Debug("Block [%d] has prepared", prepareBlockLog.blockIndex)
	}else{
		prepareBlockLog.prepared = true
		Debug("Block [%d] has prepared, but committed ahead and committed req number = %d", prepareBlockLog.blockIndex, len(prepareBlockLog.duplicatedReqs))
	}
	s.height2blockLogMu.Lock()
	commitBlockLog, ok:= s.height2blockLog[msg.CommitBlockIndex]
	s.height2blockLogMu.Unlock()
	if !ok{
		commitBlockLog := &BlockLog{
			blockIndex: msg.CommitBlockIndex,
			prepared: false,
			committed: true,
		}
		s.height2blockLogMu.Lock()
		s.height2blockLog[msg.Block.BlockIndex] = commitBlockLog
		s.height2blockLogMu.Unlock()
		Debug("Block [%d] has committed, but not prepared", commitBlockLog.blockIndex)
	}else{
		commitBlockLog.committed = true
		//Debug("Block [%d] has committed and committed req number = %d", commitBlockLog.blockIndex, len(commitBlockLog.duplicatedReqs))
	}
	returnMsg := &SendingReturnMsg{
		PrepareBlockIndex: msg.Block.BlockIndex,
		CommittedBlockIndex: args.Msg.CommitBlockIndex,
		NewDuplicatedReqs: make([]*duplicatedReqUnit, 10),
		NodeId: s.node.id,
	}
	returnMsg.NewDuplicatedReqs = nil
	s.localDuplicatedMu.Lock()
	//Debug("local duplicated reqs = %d", len(s.localDuplicatedReqs))
	for i:= 0; i < len(s.localDuplicatedReqs); i++{
		Debug("report unit tx num = %d", s.localDuplicatedReqs[i].TxNum)
		returnMsg.NewDuplicatedReqs = append(returnMsg.NewDuplicatedReqs, s.localDuplicatedReqs[i])
	}
	//Debug()
	s.localDuplicatedReqs = nil
	s.localDuplicatedMu.Unlock()
	digest = Sha256Digest(returnMsg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	returnArgs.Digest = digest
	returnArgs.Msg = returnMsg
	returnArgs.Sign = sign
	return nil
}

//主节点发送prepare消息给从节点
//Raft 第一阶段(主节点)
func (s *Server) Prepare(seq int64) {
	//共识线程从seq2cert中获取请求参数，并构造prepare消息

    //构造prepare消息------------------------
	req, digest, primary, logIndex := s.getCertOrNew(seq).get()
	msg := &PrepareMsg{
		logIndex: logIndex,
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
		TxNum: int64(req.TxNum),
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数
	args := &PrepareArgs{
		Msg:     msg,
		Sign:    sign,
		ReqArgs: req,
	}
	//--------------------------------------

	// 发送prepare消息
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var returnArgs PrepareConfirmArgs
			err := srvCli1.Call("Server.PrepareRpc", args, &returnArgs)
			if err != nil {
				Error("Server.PrepareRpc %d error: %v", id1, err)
			}
			//returnMsg := returnArgs.Msg
			//Debug("PrepareShareRpc, seq: %d, from: %d", msg.Seq, id1)
			// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
			if &returnArgs == nil{
				Error("Calling PrepareRpc method error")
			}
			cert := s.getCertOrNew(msg.Seq)
			cert.pushPrepareConfirm(&returnArgs)
			s.prepareMu.Lock()
			if cert.stage == PrepareStage{
				s.verifyBallot(cert)
			}
			s.prepareMu.Unlock()
		}()
	}
    //s.getCertOrNew(seq).set(nil, digest, view, primary)
	Debug("Prepare %d ok", seq)
	//s.Prepare(seq)
}
//从节点接收prepare消息
//Raft 第一阶段（从节点）
func (s *Server) PrepareRpc(args *PrepareArgs, returnArgs *PrepareConfirmArgs) error {
	msg := args.Msg
	//Debug("PrePrepareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)
	// 预设返回失败
	//*reply = false
	// 验证PrePrepareMsg
	//Debug("prepare view = %d, primaryNodeId = %d",msg.View, msg.PrimaryNodeId)
	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}
	//如果不是proposer
	//if !s.isProposerOrNot(int(msg.View), msg.PrimaryNodeId){
	//	fmt.Print(KConfig.ProposerIds)
	//	Debug("\nprimaryNode id = %d", msg.PrimaryNodeId)
	//	return nil
	//}
	// 验证RequestMsg
	reqArgs := args.ReqArgs
	//node = GetNode(reqArgs.Req.ClientId)
	node = GetNode(msg.NodeId)
	digest = Sha256Digest(reqArgs.Req)
	if !SliceEqual(digest, msg.Digest) {
		Warn("PrepareMsg error, req.digest != msg.Digest")
		return nil
	}
	//ok = RsaVerifyWithSha256(digest, reqArgs.Sign, node.pubKey)
	//if !ok {
	//	Warn("RequestMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
	//	return nil
	//}
	// 设置证明
	//从节点收到pre-prepare消息之后，将对应的请求存储到seq2cert中
	cert := s.getCertOrNew(msg.Seq)
	if cert.committed {
		return nil
	}
	//cert.set(reqArgs, digest, msg.View,msg.PrimaryNodeId)
	cert.set(nil, digest, msg.logIndex, msg.PrimaryNodeId)
	_, digest, primary, logIndex:= s.getCertOrNew(msg.Seq).get()

	//view := s.localViewCommitted.getView(viewNum)
	//s.sysStartToViewStart[viewNum-1] = view.startTime.Sub(s.startTime).Seconds()
	//s.viewStartTime[viewNum-1] = view.startTime

	returnMsg := &PrepareConfirmMsg{
		Seq:    cert.seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
		LogIndex: logIndex,
	}
	digest = Sha256Digest(returnMsg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	returnArgs.Msg = returnMsg
	returnArgs.Sign = sign
	return nil
}

//主节点发送commit消息
//Raft 第二阶段（主节点）
func (s *Server) Commit(seq int64) {
	req, digest, primary, logIndex := s.getCertOrNew(seq).get()
	msg := &CommitMsg{
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
		TxNum: int64(req.TxNum),
		LogIndex: logIndex,
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &CommitArgs{
		Msg:  msg,
		Sign: sign,
	}
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var returnArgs CommitConfirmArgs
			err := srvCli1.Call("Server.CommitRpc", args, &returnArgs)
			if err != nil {
				Error("Server.CommitRpc %d error: %v", id1, err)
			}
			cert := s.getCertOrNew(msg.Seq)
			cert.pushCommitConfirm(&returnArgs)
		}()
	}
}
//从节点接收commit消息
//Raft 第二阶段（从节点）
func (s *Server) CommitRpc(args *PrepareArgs, returnArgs *CommitConfirmArgs) error {
	msg := args.Msg
	cert := s.getCertOrNew(msg.Seq)

	node := GetNode(msg.PrimaryNodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("PrepareConfirmMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}

	_, digest, primary, logIndex:= s.getCertOrNew(cert.seq).get()
	returnMsg := &CommitConfirmMsg{
		Seq:    cert.seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
		LogIndex: logIndex,
	}
	digest = Sha256Digest(returnMsg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	returnArgs.Msg = returnMsg
	returnArgs.Sign = sign

	if cert.stage == PrepareStage{
		cert.stage = CommitStage
		s.currentBlockIndex++
	}
	Info("Backup[%d] has committed log[%d]",s.node.id, s.currentBlockIndex)
	return nil
}

func (s *Server) verifyBallot(cert *LogCert) {
	req, reqDigest, _, _ := cert.get()
	// cmd 为空则不进行后续阶段
	if req == nil {
		Debug("march, cmd is nil")
		return
	}
	//从duplicateQ中取出所有duplicate消息
	argsQ := cert.popAllDuplicateConfirms()
	for _, args := range argsQ {
		msg := args.Msg
		//Debug("msg nodeId = %d", msg.NodeId)
		if cert.duplicateConfirmVoted(msg.NodeId) { // 已投票
			continue
		}

		if !SliceEqual(reqDigest, msg.Digest) {
			Warn("DuplicateMsg error, req.digest != msg.Digest")
			continue
		}
		// 验证DuplicateConfirmMsg
		node := GetNode(msg.NodeId)
		digest := Sha256Digest(msg)
		ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
		if !ok {
			Warn("DuplicateConfirmMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
			continue
		}
		//投prepareConfirm票
		cert.duplicateConfirmVote(args)
	}
		// f + 1 (包括自身) 后进入 commit 阶段
		if cert.duplicateConfirmBallot() >= KConfig.FaultNum {
			//Info("Primary has duplicated request %s(hash value) ", cert.digest)
			cert.setStage(DuplicatedStage)
			duplicatedReq := &duplicatedReqUnit{
				DuplicatingNodeId: s.node.id,
				Digest: reqDigest,
				TxNum: int64(cert.req.TxNum),
				Sign: RsaSignWithSha256(reqDigest, s.node.priKey),
			}
			cert.req = nil
			s.localDuplicatedMu.Lock()
			//Debug("Broadcasted req txNum = %d", duplicatedReq.TxNum)
			s.localDuplicatedReqs = append(s.localDuplicatedReqs, duplicatedReq)
			//Debug("nodeId : ", s.localDuplicatedReqs[len(s.localDuplicatedReqs)-1].duplicatingNodeId)

			s.localDuplicatedMu.Unlock()
			//go s.Commit(cert.seq)
            //
            cert.completeTime = time.Now()
            Debug("Duplicating duration time = %d ms", cert.completeTime.Sub(cert.produceTime).Milliseconds())
			s.dupTime = append(s.dupTime, cert.completeTime.Sub(cert.produceTime).Milliseconds())
			Debug("duplicate round = %d", len(s.dupTime))
			if len(s.dupTime) == 100 {
				fmt.Print(s.dupTime)
			}
				s.makeReq()
			//s.delayReset()
			//Debug("Entering a new round of duplicating ")

		}
}

func (s *Server) Reply(seq int64) {
	Debug("Reply %d", seq)
	req, _ ,_, _ := s.getCertOrNew(seq).get()
	msg := &ReplyMsg{
		Seq:       seq,
		Timestamp: time.Now().UnixNano(),
		ClientId:  req.Req.ClientId,
		NodeId:    s.node.id,
		// Result:    req.Req.Operator,
	}
	digest := Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	replyArgs := &ReplyArgs{
		Msg:  msg,
		Sign: sign,
	}
	var reply bool
	cliCli := s.getCliCli(req.Req.ClientId)
	if cliCli == nil {
		Warn("can't connect client %d", req.Req.ClientId)
		return
	}
	err := cliCli.Call("Client.ReplyRpc", replyArgs, &reply)
	if err != nil {
		Warn("Client.ReplyRpc error: %v", err)
		s.closeCliCli(req.Req.ClientId)
	}
}

func (s *Server) getCliCli(clientId int64) *rpc.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	cliCli, ok := s.id2cliCli[clientId]
	if !ok || cliCli == nil {
		node := GetNode(clientId)
		var err error
		cliCli, err = rpc.DialHTTP("tcp", node.addr)
		if err != nil {
			Warn("connect client %d error: %v", node.addr, err)
			return nil
		}
		s.id2cliCli[clientId] = cliCli
	}
	return cliCli
}

func (s *Server) closeCliCli(clientId int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	Info("close connect with client %d", clientId)
	cliCli, ok := s.id2cliCli[clientId]
	if ok && cliCli != nil {
		cliCli = nil
		delete(s.id2cliCli, clientId)
	}
}

func (s *Server) CloseCliCliRPC(args *CloseCliCliArgs, reply *bool) error {
	s.closeCliCli(args.ClientId)
	*reply = true
	return nil
}

func (s *Server) connect() {
	ok := false
	for !ok {
		time.Sleep(time.Second) // 每隔一秒进行连接
		Info("build connect...")
		ok = true
		Debug("nodes number = %d", len(KConfig.Id2Node))
		for id, node := range KConfig.Id2Node {
			if node == s.node {
				continue
			}
			if s.id2srvCli[id] == nil {
				Debug("connect to node %s", node.addr)
				cli, err := rpc.DialHTTP("tcp", node.addr)
				if err != nil {
					Warn("connect %s error: %v", node.addr, err)
					ok = false
				} else {
					s.id2srvCli[id] = cli
				}
			}
		}
	}
	Info("== connect success ==")
}

func (s *Server) isProposerOrNot(viewNum int,nodeId int64)bool{
	if !s.rotateOrNot{
		for _,proposerId := range s.proposers{
			if nodeId == proposerId{
				return true
			}
		}
	}else{
		index := viewNum % len(KConfig.PeerIds)
		for i := 0; i < len(s.proposers);i++{
			if KConfig.PeerIds[(index + i) % len(KConfig.PeerIds)] == nodeId{
				return true
			}
		}
	}
	return false
}

func (s *Server) makeReq(){
	//time.Sleep(500*time.Millisecond)
	var realBatchTxNum int
	//s.txPoolMu.Lock()
	////如果交易池中没有交易，则循环等待。注意此处加锁逻辑是否有问题。
	//if s.txPool == 0 {
	//	s.txPoolMu.Unlock()
	//	for{
	//		time.Sleep(200*time.Millisecond)
	//		s.txPoolMu.Lock()
	//		if s.txPool != 0{
	//			break
	//		}
	//		s.txPoolMu.Unlock()
	//	}
	//}
	//if s.txPool > int64(KConfig.BatchTxNum){
	//	realBatchTxNum = KConfig.BatchTxNum
	//	s.txPool -= int64(KConfig.BatchTxNum)
	//	s.txPoolMu.Unlock()
	//} else {
	//	realBatchTxNum = int(s.txPool)
	//	s.txPool = 0
	//
	//	//Debug("txPool = %d", s.txPool)
	//	s.txPoolMu.Unlock()
	//}

	//var txPoolNum = 0
	s.txPoolMu.Lock()

	if s.txPoolBatches[len(s.txPoolBatches)-1].txNum == 0 {
		s.txPoolMu.Unlock()
		for{
			time.Sleep(500*time.Millisecond)
			s.txPoolMu.Lock()
			if s.txPoolBatches[len(s.txPoolBatches)-1].txNum != 0{
				break
			}
			s.txPoolMu.Unlock()
		}
	}
	for i := 0; i < len(s.txPoolBatches); i++{
		if int(s.txPoolBatches[i].txNum) > KConfig.BatchTxNum - realBatchTxNum{
			realBatchTxNum += KConfig.BatchTxNum - realBatchTxNum
			s.txPoolBatches[i].txNum -= int64(KConfig.BatchTxNum - realBatchTxNum)
			break
		}else{
			if s.txPoolBatches[i].txNum != 0 {
				realBatchTxNum += int(s.txPoolBatches[i].txNum)
				s.txPoolBatches[i].txNum = 0
				s.txPoolBatches[i].completeTime = time.Now()
				s.txPoolBatches[i].completed = true
				Debug("This batch latency in tx pool is %d ms", s.txPoolBatches[i].completeTime.Sub(s.txPoolBatches[i].arrivalTime).Milliseconds())
			}
		}
	}
	s.txPoolMu.Unlock()
	//if s.txPool > int64(KConfig.BatchTxNum){
	//	realBatchTxNum = KConfig.BatchTxNum
	//	s.txPool -= int64(KConfig.BatchTxNum)
	//	s.txPoolMu.Unlock()
	//} else {
	//	realBatchTxNum = int(s.txPool)
	//	s.txPool = 0
	//
	//	//Debug("txPool = %d", s.txPool)
	//	s.txPoolMu.Unlock()
	//}

	req := &RequestMsg {
		Operator:  make([]byte, realBatchTxNum*KConfig.TxSize),
		Timestamp: time.Now().UnixNano(),
		//ClientId:  args.Req.ClientId,
	}
	//构造请求
	node := GetNode(s.node.id)
	digest := Sha256Digest(req)
	sign := RsaSignWithSha256(digest, node.priKey)

	args := &RequestArgs {
		Req:  req,
		TxNum: realBatchTxNum,
		Sign: sign,
	}
	seq := s.assignSeq()
	//主节点新建logCert，设置参数，并存储在seq2cert中
	//s.view++
	//s.currentLogIndex++
	s.getCertOrNew(seq).set(args, digest, 0, s.node.id)
	s.getCertOrNew(seq).produceTime = time.Now()
	//Debug("Request %s(hash value) has been created, preparing for duplicating", digest)
	//向共识线程发送开始共识信号
	//Debug("***************** currentLogIndex = %d", s.currentLogIndex)
	s.seqCh <- seq
}

func (s *Server) workLoop() {
    startTime := time.Now()
    //if s.node.id == 14804501{
		s.makeReq()
	//}
    go s.controlSending()

	fmt.Printf("start time = %v\n ", startTime)
	for seq := range s.seqCh {
		if KConfig.DuplicateMode == 1{
			Debug("start broadcast duplicating")
			s.duplicate(seq)
		} else {
			Debug("start tree duplicating")
			s.treeDuplicate(seq)
		}
	}
}

func (s *Server)calculateTPS(){
	for i:= 0; i < 100;i++{
		timeNow := time.Now()
		s.tps[i] = float64(s.localNodeSendingTxs) / timeNow.Sub(s.startTime).Seconds()
		time.Sleep(1*time.Second)
	}
}

func (s *Server) Start() {
	s.connect()
	time.Sleep(2*time.Second)
	s.workLoop()
}

func RunServer(id int64, delayRange int64) {
	//view := View{
	//	committedInstance: make(map[int64]bool),
	//}
	views := make(map[int64]View, 0)
	var localView LocalView
	localView.views = views
	localView.currentStableViewHeight = 0

	//randomDelay, _ := rand.Int(rand.Reader, big.NewInt(int64(KConfig.Delay)))
	//rand.Seed(time.)
	//randomDelay := rand.Int(10)

	server := &Server{
		node:      KConfig.Id2Node[id],
		seqCh:     make(chan int64, ChanSize),
		logs:      make([]*Log, 0),
		eachInstanceViewLocally: make(map[int64]int64),
		viewCommittedInstance: make(map[int64]int64),
		seq2cert:  make(map[int64]*LogCert),
		id2srvCli: make(map[int64]*rpc.Client),
		id2cliCli: make(map[int64]*rpc.Client),
		localViewCommitted: localView,
		//randomDelay: randomDelay.Int64(),
		randomDelay: 0,
		startTime: time.Now(),
		proposers: make([]int64, KConfig.ProposerNum),
		isProposer: KConfig.IsProposer,
		delay: int64(KConfig.Delay),
		tps: make([]float64, 100),
		roundEndTime: make([]time.Time, 100),
		latencyPerRound: make([]float64, 100),
		viewEndTime: make([]time.Time, 100),
		viewStartTime: make([]time.Time, 100),
		latencyPerView: make([]float64, 100),
		delayPerView: make([]int64, 100),
		sysStartToViewStart: make([]float64, 100),
		sysStartToViewEnd: make([]float64, 100),
		rotateOrNot: KConfig.RotateOrNot,
		randomDelayOrNot: KConfig.RandomDelayOrNot,
        // for PRaft
		currentBlockIndex: 0,
		currentTerm: 1,
		height2blockLog:  make(map[int64]*BlockLog),
		//localDuplicatedReqs: make([]*duplicatedReqUnit,10),
	}
	server.delayReset()
	Debug("random delay is %d ms",server.randomDelay)
	for _, nodeId := range KConfig.PeerIds{
		server.eachInstanceViewLocally[nodeId] = 0
	}
	for i := 0; i < KConfig.ProposerNum;i++{
		server.proposers[i] = KConfig.ProposerIds[i]
		//Debug("proposer id = %d",server.proposers[i])
	}
	// 每个分配序号后缀为节点id(8位)
	server.seqInc = server.node.id
	// 当前暂无view-change, view暂且设置为server id
	//server.view = server.node.id
	server.view = 0
	server.txPool = 0
	server.currentConfirmedTx = 0
	server.localNodeSendingTxs = 0
	server.cumulative = 0
	server.throughput = 0

	go server.Start()
	go server.pushTxToPool()

	rpc.Register(server)
	rpc.HandleHTTP()
	if err := http.ListenAndServe(server.node.addr, nil); err != nil {
		log.Fatal("server error: ", err)
	}
}
