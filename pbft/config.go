package pbft

import (
	"fmt"
	"log"
)

const (
	delayTime   = 30 * 100000
	batchNum    = 4
	ClientDelay = 5 * 1000

	from       = 101
	to         = 102
	MBSize     = 1024 * 1024
	BatchTxNum = 4096 / 1
	TxSize     = 256

	ChanSize     = 10000
	sendTaskSize = 256

	ExecTime = 0
	ExecNum  = 1e8

	HeartbeatTime = 2

	PortBase = 10000

	// ClientIp   = "127.0.0.1"
	//ClientIp = "10.11.1.201"
	//ClientIp = "10.11.1.211"
	//ClientIp = "10.11.1.193"
	ClientIp   = "10.11.1.207"
	ClientPort = PortBase

	flowSize = 10 * MBSize
)

var (
	f          int
	BoostNum   int
	BoostDelay = 5 * 1000
	NodeNum    int

	NodeTable map[int64]*Node
	Ips       []string
	Ids       []int64

	ClientNode = NewNode(ClientIp, ClientPort, nil, nil)
)

func InitNodeAddr(ipNum, processNum int) error {

	Ips = ReadIps("./config/ips.txt")
	fmt.Println("# Ips:", Ips)

	NodeTable = make(map[int64]*Node)

	for _, ip := range Ips[:ipNum] {
		for i := 1; i <= processNum; i++ {
			port := PortBase + i
			id := GetId(ip, port)
			keyDir := "./certs/" + fmt.Sprint(id)
			priKey, pubKey := ReadKeyPair(keyDir)

			NodeTable[id] = NewNode(ip, port, priKey, pubKey)
			Ids = append(Ids, id)
		}
	}

	NodeNum = len(NodeTable)
	f = (NodeNum - 1) / 3

	fmt.Println("# network server peers:")
	for _, node := range NodeTable {
		fmt.Printf("# node id: %d, node addr: %s\n", node.id, node.getAddr())
	}
	fmt.Println("# client:", ClientNode.getAddr())
	fmt.Printf("\n# node num: %d\tf: %d\n", len(NodeTable), f)

	fmt.Println()

	return nil
}

func GetNode(id int64) *Node {
	if NodeTable == nil {
		log.Panic("NodeTable is not initialized!")
	}
	node, ok := NodeTable[id]
	if !ok {
		log.Panicf("The node of this ID(%d) does not exist!", id)
	}
	return node
}

func GetIndex(ip string) int {
	for idx, ip2 := range Ips {
		if ip == ip2 {
			return idx
		}
	}
	return -1
}
