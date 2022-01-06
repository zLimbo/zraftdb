package test

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
	"zpbft/pbft"
)

var priKey, pubKey = pbft.ReadKeyPair("../certs/1001100119301")

const ByteLen = 4

var BufSize = 1024

var totalRecvSize int64 = 0
var mutex sync.Mutex

func trafficLog() {
	start := time.Now()
	lastRecvSize := int64(0)
	for {
		time.Sleep(time.Second)
		mutex.Lock()
		gap := totalRecvSize - lastRecvSize
		lastRecvSize = totalRecvSize
		mutex.Unlock()

		speed := float64(gap / pbft.MBSize)
		spend := time.Since(start).Seconds()
		totalSpeed := float64(totalRecvSize/pbft.MBSize) / spend

		fmt.Printf("traffic speed: %0.2f MB/s\n", speed)
		fmt.Printf("total speed: %0.2f MB/s\n", totalSpeed)
	}
}

func client() {

	serverAddr := os.Args[5]
	conn, err := net.Dial("tcp4", serverAddr)

	//conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Panic(err)
	}

	for i := 0; i < 1; i++ {
		reqMsg := &pbft.Message{
			MsgType:   pbft.MtRequest,
			Seq:       1,
			NodeId:    0,
			Timestamp: time.Now().UnixNano(),
			Txs:       new(pbft.BatchTx),
		}
		reqMsgBytes, _ := json.Marshal(reqMsg)
		fmt.Println("reqMsg size:", float64(len(reqMsgBytes))/float64(pbft.MBSize))

		ppMsg := &pbft.Message{
			MsgType: pbft.MtPrePrepare,
			Seq:     int64(i),
			NodeId:  1,

			Timestamp: time.Now().UnixNano(),
			Req:       reqMsg,
		}
		ppMsgBytes, _ := json.Marshal(ppMsg)
		fmt.Println("ppMsg size:", float64(len(ppMsgBytes))/float64(pbft.MBSize), "MB")

		//signMsg := pbft.SignMsg(ppMsg, priKey)
		//signMsgBytes, _ := json.Marshal(signMsg)
		//result := pbft.VerifySignMsg(signMsg, pubKey)
		//fmt.Println("result:", result)
		//fmt.Println("signMsg byte size:", len(signMsgBytes))

		sendSize := 1000
		if len(os.Args) > 6 {
			sendSize, _ = strconv.Atoi(os.Args[6])
		}
		signMsgBytes := make([]byte, sendSize*pbft.MBSize)
		sz := len(signMsgBytes)
		szBytes := pbft.I2Bytes(int64(sz), ByteLen)
		fmt.Println("szBytes:", szBytes)
		start := time.Now()
		data := append(szBytes, signMsgBytes...)
		fmt.Println("append spend:", time.Since(start))

		start = time.Now()
		n, err := conn.Write(data)
		if err != nil {
			fmt.Println("err:", err)
		}
		fmt.Println("n:", n, "sz:", sz)

		buffer := make([]byte, 1e8)
		n, _ = conn.Read(buffer)
		fmt.Println("response:", string(buffer[:n]))
		fmt.Println("size:", sendSize, "MB, spend:", time.Since(start))
		fmt.Printf("speed: %0.2f MB/s\n", float64(sendSize)/time.Since(start).Seconds())
	}
	conn.Close()
}

type ConnMgr struct {
	conn    net.Conn
	status  int
	msgLen  int
	pos     int
	buf     []byte
	bufChan chan []byte
}

var Mgr = &ConnMgr{
	conn:    nil,
	status:  0,
	msgLen:  0,
	pos:     0,
	buf:     make([]byte, 0),
	bufChan: make(chan []byte),
}

func recvMsg() {

	for buf := range Mgr.bufChan {
		Mgr.buf = append(Mgr.buf, buf...)

		for {
			if len(Mgr.buf) < ByteLen {
				break
			}
			//fmt.Println("sizeByte:", Mgr.buf[:ByteLen])
			size := pbft.Bytes2I(Mgr.buf[:ByteLen], ByteLen)
			if len(Mgr.buf) < ByteLen+int(size) {
				break
			}
			//signMsg := new(pbft.SignMessage)
			//err := json.Unmarshal(Mgr.buf[ByteLen:ByteLen+size], signMsg)
			//if err != nil {
			//	log.Panic(err)
			//}
			//fmt.Println("signMsg Seq:", signMsg.Msg.Seq)
			//result := pbft.VerifySignMsg(signMsg, pubKey)
			//fmt.Println("result:", result)
			//(*Mgr.conn).Write([]byte("ok seq" + strconv.Itoa(int(signMsg.Msg.Seq))))
			fmt.Println("size:", size, " bytes")
			Mgr.conn.Write([]byte("ok"))
			Mgr.buf = Mgr.buf[ByteLen+size:]

			select {}
		}
	}
}

func server() {
	addr := os.Args[5]
	if len(os.Args) > 6 {
		BufSize, _ = strconv.Atoi(os.Args[6])
	}
	fmt.Println("BufSize:", BufSize)
	fmt.Println(addr, "listen...")
	// listen, err := net.Listen("tcp", addr)
	listen, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Panic(err)
	}
	defer listen.Close()

	conn, err := listen.Accept()
	if err != nil {
		log.Panic(err)
	}

	fmt.Println("#### conn")
	fmt.Println(conn.RemoteAddr().String())
	fmt.Println(conn.LocalAddr().String())
	fmt.Println("#### conn")
	Mgr.conn = conn

	go recvMsg()
	go trafficLog()

	over := false
	for {
		buf := make([]byte, BufSize)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err)
		}
		//fmt.Println("n:", n)
		if n == 0 {
			if over {
				break
			}
			over = true
		}

		Mgr.bufChan <- buf[:n]
		mutex.Lock()
		totalRecvSize += int64(n)
		mutex.Unlock()
	}

	conn.Close()
}

func TestLongTermConn(t *testing.T) {
	fmt.Println(os.Args[4:])
	name := os.Args[4]
	if name == "server" {
		go server()
	} else if name == "client" {
		go client()
	} else {
		fmt.Println("parameter error!")
	}

	select {}
}
