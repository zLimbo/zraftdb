package pbft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

type Config struct {
	PeerIps  []string `json:"peerIps"`
	ClientIp string   `json:"clientIp"`
	// IpNum      int      `json:"ipNum"`
	// ProcessNum int      `json:"processNum"`
	// ReqNum     int      `json:"reqNum"`
	// StartDelay int      `json:"startDelay"`
}

const kConfigFile = "config/config.json"

var KConfig Config

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
	// ClientIp   = "10.11.1.207"
	// ClientPort = PortBase

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

	ClientNode *Node
)

func InitConfig(ipNum, processNum int) {

	jsonBytes, err := ioutil.ReadFile(kConfigFile)
	if err != nil {
		log.Panicln(err)
	}
	// log.Println("| config: ", string(jsonBytes))
	err = json.Unmarshal(jsonBytes, &KConfig)
	if err != nil {
		log.Panicln(err)
	}

	Ips = KConfig.PeerIps
	NodeTable = make(map[int64]*Node)

	ClientNode = NewNode(KConfig.ClientIp, PortBase, nil, nil)

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

	log.Println("| network server peers:")
	for _, node := range NodeTable {
		log.Printf("| node id: %d | node addr: %s", node.id, node.GetAddr())
	}
	log.Println("| client addr:", ClientNode.GetAddr())
	log.Printf("| node num: %d | f: %d", len(NodeTable), f)
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
