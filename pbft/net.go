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
	kSendLenByteSize = 4
	kIdByteSize      = 8
	kTryBuildConnNum = 3
)

var (
	kSignMsgChan   = make(chan *SignMessage, ChanSize)
	kWaitConnChan  = make(chan *Node, ChanSize)
	kSendFailCount = 0
	kNotConnCount    = 0
)

type ConnMgr struct {
	node     *Node
	tcpConn  net.Conn
	recvBuf  []byte
	recvChan chan []byte
	sendChan chan []byte
	mutex    sync.Mutex
}

func NewConnMgr(node *Node) *ConnMgr {
	connMgr := &ConnMgr{
		node:     node,
		tcpConn:  nil,
		recvBuf:  make([]byte, 0),
		recvChan: make(chan []byte, 1),
		sendChan: make(chan []byte, ChanSize),
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
		Debug("connect ok, local: %s, remote: %s", tcpConn.LocalAddr(), tcpConn.RemoteAddr())
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
	for bytes := range connMgr.sendChan {

		sz := len(bytes)
		szBytes := I2Bytes(int64(sz), kSendLenByteSize)
		data := append(szBytes, bytes...)

		if connMgr.getTcpConn() == nil {
			connMgr.sendChan <- bytes
			return
		}

		_, err := connMgr.write(data)
		if err != nil {
			Warn("connMgr.write(data), err: %v", err)
			kSendFailCount++
			connMgr.closeTcpConn()
			connMgr.sendChan <- bytes
			return
		}
	}
}

func (connMgr *ConnMgr) recv() {
	for {
		buf := make([]byte, KConfig.RecvBufSize)
		if connMgr.getTcpConn() == nil {
			return
		}
		n, err := connMgr.read(buf)
		if err != nil {
			Warn("connMgr.read(buf), err: %v", err)
			connMgr.closeTcpConn()
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
		connMgr.recvBuf = append(connMgr.recvBuf, buf...)
		buf = nil
		for {
			if len(connMgr.recvBuf) < kSendLenByteSize {
				break
			}
			size := Bytes2I(connMgr.recvBuf[:kSendLenByteSize], kSendLenByteSize)
			if len(connMgr.recvBuf) < kSendLenByteSize+int(size) {
				break
			}
			var signMsg SignMessage
			err := json.Unmarshal(connMgr.recvBuf[kSendLenByteSize:kSendLenByteSize+size], &signMsg)
			if err != nil {
				Error("json.Unmarshal(...), err: %v", err)
			}
			kSignMsgChan <- &signMsg
			connMgr.recvBuf = connMgr.recvBuf[kSendLenByteSize+size:]
		}
	}
}

func (replica *Replica) keep() {
	for {
		kNotConnCount = 0
		if KConfig.ClientNode.connMgr.getTcpConn() == nil {
			kNotConnCount++
			kWaitConnChan <- KConfig.ClientNode
		}
		for id, node := range KConfig.Id2Node {
			if id == replica.node.id {
				continue
			}
			if node.connMgr.getTcpConn() == nil {
				kNotConnCount++
				kWaitConnChan <- node
			}
		}
		if kNotConnCount != 0 {
			time.Sleep(time.Second * 1)
		} else {
			time.Sleep(time.Second * 5)
		}
	}
}

func (replica *Replica) listen() {
	addr := "0.0.0.0:" + strconv.Itoa(replica.node.port)
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		Error("net.Listen(\"tcp4\", addr), err: %v", err)
	}
	defer listener.Close()
	Debug(replica.node.addr + "listen...")
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
	buf := make([]byte, kIdByteSize)
	for i := 0; i < kTryBuildConnNum; i++ {
		n, err := tcpConn.Read(buf)
		if err != nil {
			break
		}
		if n == 0 {
			runtime.Gosched()
			continue
		}
		if n != kIdByteSize {
			tcpConn.Write([]byte{0})
			break
		}
		nodeId := Bytes2I(buf, kIdByteSize)
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
	for node := range kWaitConnChan {
		if node.connMgr.getTcpConn() != nil {
			continue
		}
		Debug("dial %s ...", node.addr)
		tcpConn, err := net.Dial("tcp4", node.addr)
		if err != nil {
			Warn("dial %s failed", node.addr)
			continue
		}
		replica.connectBuildConn(tcpConn, node)
	}
}

func (replica *Replica) connectBuildConn(tcpConn net.Conn, node *Node) {
	idBytes := I2Bytes(replica.node.id, kIdByteSize)
	tcpConn.Write(idBytes)
	for i := 0; i < kTryBuildConnNum; i++ {
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

func (replica *Replica) connStatus() {
	time.Sleep(3 * time.Second)
	for {
		ok := true
		Info("==== connect status ====")
		for _, node := range KConfig.Id2Node {
			if replica.node == node {
				continue
			}
			if node.connMgr.getTcpConn() != nil {
				Info("[connect ok] %s", node.addr)
			} else {
				Warn("[connect fail] %s", node.addr)
				ok = false
			}
		}
		if !IsClient() {
			if KConfig.ClientNode.connMgr.getTcpConn() != nil {
				Info("[connect ok] %s", KConfig.ClientNode.addr)
			} else {
				Warn("[connect fail] %s", KConfig.ClientNode.addr)
				ok = false
			}
		}
		Info("========================")
		if ok {
			time.Sleep(30 * time.Second)
		} else {
			time.Sleep(5 * time.Second)
		}

	}
}
