package pbft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	MsgSizeByteNum = 4
	IdByteNum      = 8
	BuildConnNum   = 10
	RecvBufSize    = 2048
)

var (
	recvChan       = make(chan *SignMessage, ChanSize)
	sendErrorCount = 0

	connectChan = make(chan *Node, ChanSize)
	noConnCnt   = 0

	flowCtlChan = make(chan byte, flowSize)
	flowCtlTime time.Duration
)

type FlowLog struct {
	inSize, outSize   int64
	inMutex, outMutex sync.Mutex
}

func (f *FlowLog) in(size int64) {
	f.inMutex.Lock()
	defer f.inMutex.Unlock()
	f.inSize += size
}

func (f *FlowLog) out(size int64) {
	f.outMutex.Lock()
	defer f.outMutex.Unlock()
	f.outSize += size
}

func (f *FlowLog) getIn() int64 {
	f.inMutex.Lock()
	defer f.inMutex.Unlock()
	return f.inSize
}

func (f *FlowLog) getOut() int64 {
	f.inMutex.Lock()
	defer f.inMutex.Unlock()
	return f.outSize
}

type NetMgr struct {
	node     *Node
	tcpConn  net.Conn
	recvBuf  []byte
	recvChan chan []byte
	sendChan chan *SignMessage
	mutex    sync.Mutex
	flowLog  *FlowLog
}

func NewNetMgr(node *Node) *NetMgr {
	netMgr := &NetMgr{
		node:     node,
		tcpConn:  nil,
		recvBuf:  make([]byte, 0),
		recvChan: make(chan []byte, 1),
		sendChan: make(chan *SignMessage, ChanSize),
		mutex:    sync.Mutex{},
		flowLog:  new(FlowLog),
	}
	go netMgr.handleMsg()
	return netMgr
}

func (netMgr *NetMgr) getTcpConn() net.Conn {
	netMgr.mutex.Lock()
	defer netMgr.mutex.Unlock()
	return netMgr.tcpConn
}

func (netMgr *NetMgr) setTcpConn(tcpConn net.Conn) bool {
	netMgr.mutex.Lock()
	defer netMgr.mutex.Unlock()
	if netMgr.tcpConn != nil {
		fmt.Println("connect alread established!")
		return false
	}
	netMgr.tcpConn = tcpConn

	fmt.Println("###")
	fmt.Println("# local:", tcpConn.LocalAddr())
	fmt.Println("# remote:", tcpConn.RemoteAddr())
	fmt.Println("### connect success!")

	if netMgr.tcpConn != nil {
		go netMgr.send()
		go netMgr.recv()
	}
	return true
}

func (netMgr *NetMgr) closeTcpConn() {
	netMgr.mutex.Lock()
	defer netMgr.mutex.Unlock()
	if netMgr.tcpConn == nil {
		fmt.Println("connect nil!")
		return
	}
	netMgr.tcpConn.Close()
	netMgr.tcpConn = nil
}

func (netMgr *NetMgr) read(buf []byte) (int, error) {
	//netMgr.mutex.Lock()
	//defer netMgr.mutex.Unlock()
	if netMgr.tcpConn == nil {
		return 0, errors.New("connect nil")
	}

	return netMgr.tcpConn.Read(buf)
}

func (netMgr *NetMgr) write(buf []byte) (int, error) {
	//netMgr.mutex.Lock()
	//defer netMgr.mutex.Unlock()
	if netMgr.tcpConn == nil {
		return 0, errors.New("connect nil")
	}
	start := time.Now()
	flowCtlTime += time.Since(start)
	n, err := netMgr.tcpConn.Write(buf)
	start = time.Now()
	flowCtlTime += time.Since(start)
	return n, err
}

func (netMgr *NetMgr) send() {
	for signMsg := range netMgr.sendChan {
		signMsgBytes, err := json.Marshal(signMsg)
		if err != nil {
			log.Panic(err)
		}
		sz := len(signMsgBytes)
		szBytes := I2Bytes(int64(sz), MsgSizeByteNum)
		data := append(szBytes, signMsgBytes...)

		_, err = netMgr.write(data)
		if err != nil {
			log.Println("err:", err)
			sendErrorCount++
			netMgr.closeTcpConn()
			netMgr.sendChan <- signMsg
			return
		}
		netMgr.flowLog.out(int64(len(data)))
		//fmt.Println("send", n, "bytes, msg type:", signMsg.Msg.MsgType, "msg seq:", signMsg.Msg.Seq)
	}
}

func (netMgr *NetMgr) recv() {

	for {
		// netMgr.mutex.Lock()
		// defer netMgr.mutex.Unlock()
		if netMgr.tcpConn == nil {
			return
		}
		buf := make([]byte, RecvBufSize)
		n, err := netMgr.read(buf)
		if err != nil {
			sendErrorCount++
			log.Println("[close] node:", netMgr.node.id, " num:", sendErrorCount)
			netMgr.closeTcpConn()
			//if err == io.EOF && netMgr.node == ClientNode {
			//	fmt.Println("====> Exit")
			//	os.Exit(-1)
			//}
			return
		}

		if n > 0 {
			netMgr.recvChan <- buf[:n]
			netMgr.flowLog.in(int64(n))
		}
	}
}

func (netMgr *NetMgr) handleMsg() {
	for buf := range netMgr.recvChan {
		// netMgr.mutex.Lock()
		// defer netMgr.mutex.Unlock()
		netMgr.recvBuf = append(netMgr.recvBuf, buf...)
		buf = nil
		for {
			if len(netMgr.recvBuf) < MsgSizeByteNum {
				break
			}
			size := Bytes2I(netMgr.recvBuf[:MsgSizeByteNum], MsgSizeByteNum)
			if len(netMgr.recvBuf) < MsgSizeByteNum+int(size) {
				break
			}
			signMsg := new(SignMessage)
			err := json.Unmarshal(netMgr.recvBuf[MsgSizeByteNum:MsgSizeByteNum+size], signMsg)
			if err != nil {
				log.Panic(err)
			}
			recvChan <- signMsg
			netMgr.recvBuf = netMgr.recvBuf[MsgSizeByteNum+size:]
		}
	}
}

func (pbft *Pbft) keep() {
	for {
		noConnCnt = 0
		if ClientNode.netMgr.getTcpConn() == nil {
			noConnCnt++
			connectChan <- ClientNode
		}
		for id, node := range NodeTable {
			if id == pbft.node.id {
				continue
			}
			if node.netMgr.getTcpConn() == nil {
				noConnCnt++
				connectChan <- node
			}
		}
		fmt.Println("#### noConnCnt:", noConnCnt)
		if noConnCnt != 0 {
			time.Sleep(time.Second * 2)
		} else {
			time.Sleep(time.Second * 8)
		}
	}
}

func (pbft *Pbft) listen() {
	addr := "0.0.0.0:" + strconv.Itoa(pbft.node.port)
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Panic(err)
	}
	defer listener.Close()
	fmt.Println(pbft.node.getAddr(), "server listen...")
	for {
		tcpConn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go pbft.listenBuildConn(tcpConn)
	}
}

func (pbft *Pbft) listenBuildConn(tcpConn net.Conn) {
	buf := make([]byte, 8)
	for i := 0; i < BuildConnNum; i++ {
		n, err := tcpConn.Read(buf)
		if err != nil {
			break
		}
		if n == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		nodeId := Bytes2I(buf, IdByteNum)
		node := GetNode(nodeId)
		ok := node.netMgr.setTcpConn(tcpConn)
		if !ok {
			tcpConn.Write([]byte{0})
			break
		}
		tcpConn.Write([]byte{1})
		return
	}
	tcpConn.Close()
}

func (pbft *Pbft) connect() {
	for node := range connectChan {
		if node.netMgr.getTcpConn() != nil {
			continue
		}
		fmt.Println("### connect ", node.getAddr(), "...")
		tcpConn, err := net.Dial("tcp4", node.getAddr())
		if err != nil {
			log.Println(err)
			log.Println("conn:", node.netMgr.getTcpConn())
			continue
		}
		pbft.connectBuildConn(tcpConn, node)
	}
}

func (pbft *Pbft) connectBuildConn(tcpConn net.Conn, node *Node) {
	buf := I2Bytes(pbft.node.id, IdByteNum)
	tcpConn.Write(buf)
	for i := 0; i < BuildConnNum; i++ {
		n, err := tcpConn.Read(buf)
		if err != nil {
			break
		}
		if n == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if buf[0] == 0 {
			break
		}
		node.netMgr.setTcpConn(tcpConn)
		return
	}
	tcpConn.Close()
}

func (pbft *Pbft) flowStatistics() {
	var inSize, outSize int64
	start := time.Now()
	gapTime := 3 * time.Second
	var inPeekSpeed, outPeekSpeed float64
	for {
		time.Sleep(gapTime)
		var inNewSize, outNewSize int64
		for id, node := range NodeTable {
			if id == pbft.node.id {
				continue
			}
			inNewSize += node.netMgr.flowLog.getIn()
			outNewSize += node.netMgr.flowLog.getOut()
		}
		totalTime := time.Since(start)
		inTotalSpeed := float64(inNewSize) / totalTime.Seconds() / MBSize
		outTotalSpeed := float64(outNewSize) / totalTime.Seconds() / MBSize
		inGap := inNewSize - inSize
		outGap := outNewSize - outSize
		inGapSpeed := float64(inGap) / gapTime.Seconds() / MBSize
		outGapSpeed := float64(outGap) / gapTime.Seconds() / MBSize
		if inGapSpeed > inPeekSpeed {
			inPeekSpeed = inGapSpeed
		}
		if outGapSpeed > outPeekSpeed {
			outPeekSpeed = outGapSpeed
		}

		fmt.Println("\n===== flow log =====")
		fmt.Printf("[in]\ttotal: %0.4fMB\ttotal_speed: %0.4fMB/s\tgap: %0.4fMB\tspeed: %0.4fMB/s\tpeek: %0.4fMB/s\n",
			float64(inNewSize)/MBSize, inTotalSpeed, float64(inGap)/MBSize, inGapSpeed, inPeekSpeed)
		fmt.Printf("[out]\ttotal: %0.4fMB\ttotal_speed: %0.4fMB/s\tgap: %0.4fMB\tspeed: %0.4fMB/s\tpeek: %0.4fMB/s\n",
			float64(outNewSize)/MBSize, outTotalSpeed, float64(outGap)/MBSize, outGapSpeed, outPeekSpeed)
		fmt.Println()

		inSize, outSize = inNewSize, outNewSize
	}
}
