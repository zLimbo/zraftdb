package pbft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

const (
	MBSize        = 1024 * 1024
	BatchTxNum    = 4096 / 1
	TxSize        = 256
	ChanSize      = 10000
	ExecTime      = 0
	ExecNum       = 1e8
	HeartbeatTime = 2
	FlowSize      = 10 * MBSize
	KConfigFile   = "./config/config.json"
	KCertsDir     = "./certs"
	KLocalIpFile  = "./config/local_ip.txt"
)

type Config struct {
	PeerIps     []string `json:"peerIps"`
	ClientIp    string   `json:"clientIp"`
	IpNum       int      `json:"ipNum"`
	PortBase    int      `json:"portBase"`
	ProcessNum  int      `json:"processNum"`
	ReqNum      int      `json:"reqNum"`
	BoostNum    int      `json:"boostNum"`
	StartDelay  int      `json:"startDelay"`
	RecvBufSize int      `json:"recvBufSize"`
	LogStdout   bool     `json:"logStdout"`

	Id2Node    map[int64]*Node
	ClientNode *Node
	PeerIds    []int64
	LocalIp    string
	FalultNum  int
}

var KConfig Config

func InitConfig(configFile string) {

	// 读取 json
	jsonBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		Panic("read %s failed.", configFile)
	}
	Info("config: ", string(jsonBytes))
	err = json.Unmarshal(jsonBytes, &KConfig)
	if err != nil {
		Panic("json.Unmarshal(jsonBytes, &KConfig) err: %v", err)
	}

	// 配置节点ip, port, 公私钥
	KConfig.PeerIps = KConfig.PeerIps[:KConfig.IpNum]
	KConfig.Id2Node = make(map[int64]*Node)
	for _, ip := range KConfig.PeerIps {
		for i := 0; i < KConfig.ProcessNum; i++ {
			port := KConfig.PortBase + 1 + i
			id := GetId(ip, port)
			keyDir := KCertsDir + "/" + fmt.Sprint(id)
			priKey, pubKey := ReadKeyPair(keyDir)

			KConfig.Id2Node[id] = NewNode(ip, port, priKey, pubKey)
			KConfig.PeerIds = append(KConfig.PeerIds, id)
		}
	}

	// 计算容错数
	KConfig.FalultNum = (len(KConfig.Id2Node) - 1) / 3

	// 设置本地IP和客户端
	KConfig.ClientNode = NewNode(KConfig.ClientIp, KConfig.PortBase, nil, nil)
	KConfig.LocalIp = GetLocalIp()

	// 发送请求的节点数
	if KConfig.BoostNum == -1 {
		KConfig.BoostNum = KConfig.IpNum * KConfig.ProcessNum
	}

	Info("config ok. KConfig:", KConfig)
}

func IsClient() bool {
	return KConfig.LocalIp == KConfig.ClientIp
}

func GetNode(id int64) *Node {
	if KConfig.Id2Node == nil {
		Panic("KConfig.Id2Node is not initialized!")
	}
	node, ok := KConfig.Id2Node[id]
	if !ok {
		Panic("The node of this ID(%d) does not exist!", id)
	}
	return node
}

func GetIndex(ip string) int {
	for idx, ip1 := range KConfig.PeerIps {
		if ip == ip1 {
			return idx
		}
	}
	return -1
}
