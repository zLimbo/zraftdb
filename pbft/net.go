package pbft

import (
	"encoding/json"
	"errors"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	MsgSizeByteSize = 4
	IdByteSize      = 8
	TryBuildConnNum = 10
)

var (
	recvChan       = make(chan *SignMessage, ChanSize)
	connectChan    = make(chan *Node, ChanSize)
	sendErrorCount = 0
	noConnCnt      = 0
)

type ConnMgr struct {
	node     *Node
	tcpConn  net.Conn
	recvBuf  []byte
	recvChan chan []byte
	sendChan chan *SignMessage
	mutex    sync.Mutex
}

func NewConnMgr(node *Node) *ConnMgr {
	connMgr := &ConnMgr{
		node:     node,
		tcpConn:  nil,
		recvBuf:  make([]byte, 0),
		recvChan: make(chan []byte, 1),
		sendChan: make(chan *SignMessage, ChanSize),
		mutex:    sync.Mutex{},
	}
	go connMgr.handleMsg()
	return connMgr
}

func (connMgr *ConnMgr) getTcpConn() net.Conn {
	connMgr.mutex.Lock()
	defer connMgr.mutex.Unlock()
	return connMgr.tcpConn
}

func (connMgr *ConnMgr) setTcpConn(tcpConn net.Conn) bool {
	connMgr.mutex.Lock()
	defer connMgr.mutex.Unlock()
	if connMgr.tcpConn != nil {
		return false
	}
	connMgr.tcpConn = tcpConn
	if connMgr.tcpConn != nil {
		Info("connect ok, local: %s, remote: %s", tcpConn.LocalAddr(), tcpConn.RemoteAddr())
		go connMgr.send()
		go connMgr.recv()
	}
	return true
}

func (connMgr *ConnMgr) closeTcpConn() {
	connMgr.mutex.Lock()
	defer connMgr.mutex.Unlock()
	if connMgr.tcpConn == nil {
		return
	}
	connMgr.tcpConn.Close()
	connMgr.tcpConn = nil
}

func (connMgr *ConnMgr) read(buf []byte) (int, error) {
	//connMgr.mutex.Lock()
	//defer connMgr.mutex.Unlock()
	if connMgr.tcpConn == nil {
		return 0, errors.New("connMgr.tcpConn == nil")
	}
	return connMgr.tcpConn.Read(buf)
}

func (connMgr *ConnMgr) write(buf []byte) (int, error) {
	//connMgr.mutex.Lock()
	//defer connMgr.mutex.Unlock()
	if connMgr.tcpConn == nil {
		return 0, errors.New("connMgr.tcpConn == nil")
	}
	return connMgr.tcpConn.Write(buf)
}

func (connMgr *ConnMgr) send() {
	for signMsg := range connMgr.sendChan {
		signMsgBytes, err := json.Marshal(signMsg)
		if err != nil {
			Panic("json.Marshal(signMsg), err: %v", err)
		}
		sz := len(signMsgBytes)
		szBytes := I2Bytes(int64(sz), MsgSizeByteSize)
		data := append(szBytes, signMsgBytes...)

		_, err = connMgr.write(data)
		if err != nil {
			Info("err:", err)
			sendErrorCount++
			connMgr.closeTcpConn()
			connMgr.sendChan <- signMsg
			return
		}
		//Info("send", n, "bytes, msg type:", signMsg.Msg.MsgType, "msg seq:", signMsg.Msg.Seq)
	}
}

func (connMgr *ConnMgr) recv() {

	for {
		// connMgr.mutex.Lock()
		// defer connMgr.mutex.Unlock()
		if connMgr.tcpConn == nil {
			return
		}
		buf := make([]byte, KConfig.RecvBufSize)
		n, err := connMgr.read(buf)
		if err != nil {
			sendErrorCount++
			Info("[close] node:", connMgr.node.id, " num:", sendErrorCount)
			connMgr.closeTcpConn()
			//if err == io.EOF && connMgr.node == KConfig.ClientNode {
			//	Info("====> Exit")
			//	os.Exit(-1)
			//}
			return
		}

		if n == 0 {
			runtime.Gosched()
		}

		connMgr.recvChan <- buf[:n]
	}
}

func (connMgr *ConnMgr) handleMsg() {
	for buf := range connMgr.recvChan {
		// connMgr.mutex.Lock()
		// defer connMgr.mutex.Unlock()
		connMgr.recvBuf = append(connMgr.recvBuf, buf...)
		buf = nil
		for {
			if len(connMgr.recvBuf) < MsgSizeByteSize {
				break
			}
			size := Bytes2I(connMgr.recvBuf[:MsgSizeByteSize], MsgSizeByteSize)
			if len(connMgr.recvBuf) < MsgSizeByteSize+int(size) {
				break
			}
			signMsg := new(SignMessage)
			err := json.Unmarshal(connMgr.recvBuf[MsgSizeByteSize:MsgSizeByteSize+size], signMsg)
			if err != nil {
				Panic("err: %v", err)
			}
			recvChan <- signMsg
			connMgr.recvBuf = connMgr.recvBuf[MsgSizeByteSize+size:]
		}
	}
}

func (replica *Replica) keep() {
	for {
		noConnCnt = 0
		if KConfig.ClientNode.connMgr.getTcpConn() == nil {
			noConnCnt++
			connectChan <- KConfig.ClientNode
		}
		for id, node := range KConfig.Id2Node {
			if id == replica.node.id {
				continue
			}
			if node.connMgr.getTcpConn() == nil {
				noConnCnt++
				connectChan <- node
			}
		}
		Info("noConnCnt:", noConnCnt)
		if noConnCnt != 0 {
			time.Sleep(time.Second * 2)
		} else {
			time.Sleep(time.Second * 8)
		}
	}
}

func (replica *Replica) listen() {
	addr := "0.0.0.0:" + strconv.Itoa(replica.node.port)
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		Panic("net.Listen(\"tcp4\", addr), err: %v", err)
	}
	defer listener.Close()
	Info(replica.node.GetAddr() + "listen...")
	for {
		tcpConn, err := listener.Accept()
		if err != nil {
			Warn("err: %v", err)
			continue
		}
		go replica.listenBuildConn(tcpConn)
	}
}

func (replica *Replica) listenBuildConn(tcpConn net.Conn) {
	buf := make([]byte, IdByteSize)
	for i := 0; i < TryBuildConnNum; i++ {
		n, err := tcpConn.Read(buf)
		if err != nil {
			break
		}
		if n == 0 {
			runtime.Gosched()
			continue
		}
		if n != IdByteSize {
			tcpConn.Write([]byte{0})
			break
		}
		nodeId := Bytes2I(buf, IdByteSize)
		node := GetNode(nodeId)
		ok := node.connMgr.setTcpConn(tcpConn)
		if !ok {
			tcpConn.Write([]byte{0})
			break
		}
		tcpConn.Write([]byte{1})
		return
	}
	tcpConn.Close()
}

func (replica *Replica) connect() {
	for node := range connectChan {
		if node.connMgr.getTcpConn() != nil {
			continue
		}
		Info("dial ", node.GetAddr(), "...")
		tcpConn, err := net.Dial("tcp4", node.GetAddr())
		if err != nil {
			Info("dial failed, conn:", node.connMgr.getTcpConn())
			continue
		}
		replica.connectBuildConn(tcpConn, node)
	}
}

func (replica *Replica) connectBuildConn(tcpConn net.Conn, node *Node) {
	idBytes := I2Bytes(replica.node.id, IdByteSize)
	tcpConn.Write(idBytes)
	for i := 0; i < TryBuildConnNum; i++ {
		buf := make([]byte, 1)
		n, err := tcpConn.Read(buf)
		if err != nil {
			break
		}
		if n == 0 {
			runtime.Gosched()
			continue
		}
		if buf[0] == 0 {
			break
		}
		if !node.connMgr.setTcpConn(tcpConn) {
			break
		}
		return
	}
	tcpConn.Close()
}
