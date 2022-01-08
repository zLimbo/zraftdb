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
	sendLenByteSize = 4
	IdByteSize      = 8
	TryBuildConnNum = 3
)

var (
	signMsgChan   = make(chan *SignMessage, ChanSize)
	waitConnChan  = make(chan *Node, ChanSize)
	sendFailCount = 0
	notConnCnt    = 0
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
	for bytes := range connMgr.sendChan {

		sz := len(bytes)
		szBytes := I2Bytes(int64(sz), sendLenByteSize)
		data := append(szBytes, bytes...)

		if connMgr.getTcpConn() == nil {
			connMgr.sendChan <- bytes
			return
		}

		_, err := connMgr.write(data)
		if err != nil {
			Warn("connMgr.write(data), err: %v", err)
			sendFailCount++
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
			if len(connMgr.recvBuf) < sendLenByteSize {
				break
			}
			size := Bytes2I(connMgr.recvBuf[:sendLenByteSize], sendLenByteSize)
			if len(connMgr.recvBuf) < sendLenByteSize+int(size) {
				break
			}
			var signMsg SignMessage
			err := json.Unmarshal(connMgr.recvBuf[sendLenByteSize:sendLenByteSize+size], &signMsg)
			if err != nil {
				Panic("json.Unmarshal(...), err: %v", err)
			}
			signMsgChan <- &signMsg
			connMgr.recvBuf = connMgr.recvBuf[sendLenByteSize+size:]
		}
	}
}

func (replica *Replica) keep() {
	go replica.ConnStatus()
	for {
		notConnCnt = 0
		if KConfig.ClientNode.connMgr.getTcpConn() == nil {
			notConnCnt++
			waitConnChan <- KConfig.ClientNode
		}
		for id, node := range KConfig.Id2Node {
			if id == replica.node.id {
				continue
			}
			if node.connMgr.getTcpConn() == nil {
				notConnCnt++
				waitConnChan <- node
			}
		}
		Info("current not connect count: %d", notConnCnt)
		if notConnCnt != 0 {
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
		Panic("net.Listen(\"tcp4\", addr), err: %v", err)
	}
	defer listener.Close()
	Info(replica.node.addr + "listen...")
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
	for node := range waitConnChan {
		if node.connMgr.getTcpConn() != nil {
			continue
		}
		Info("dial %s ...", node.addr)
		tcpConn, err := net.Dial("tcp4", node.addr)
		if err != nil {
			Info("dial %s failed", node.addr)
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

func (replica *Replica) ConnStatus() {
	time.Sleep(3 * time.Second)
	for {
		Info("==== connect status ====")
		for _, node := range KConfig.Id2Node {
			if node.connMgr.getTcpConn() != nil {
				Info("== [connect ok] %s", node.addr)
			} else {
				Warn("== [connect fail] %s", node.addr)
			}
		}
		Info("====")
		time.Sleep(10 * time.Second)
	}
}
