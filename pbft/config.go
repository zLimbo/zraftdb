package pbft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

const (
	MBSize       = 1024 * 1024
	ChanSize     = 10000
	ExecTime     = 0
	ExecNum      = 1e8
	KConfigFile  = "./config/config.json"
	KCertsDir    = "./certs"
	KLocalIpFile = "./config/local_ip.txt"
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
	LogLevel    LogLevel `json:"logLevel"`
	GoMaxProcs  int      `json:"goMaxProcs"`
	BatchTxNum  int      `json:"batchTxNum"`
	TxSize      int      `json:"txSize"`

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
		Error("read %s failed.", configFile)
	}
	Trace("config: ", string(jsonBytes))
	err = json.Unmarshal(jsonBytes, &KConfig)
	if err != nil {
		Error("json.Unmarshal(jsonBytes, &KConfig) err: %v", err)
	}

	// 配置节点ip, port, 公私钥
	KConfig.PeerIps = KConfig.PeerIps[:KConfig.IpNum]
	KConfig.Id2Node = make(map[int64]*Node)
	for i := 0; i < KConfig.ProcessNum; i++ {
		for _, ip := range KConfig.PeerIps {
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

	Info("config ok. KConfig: %v", KConfig)
}

func IsClient() bool {
	return KConfig.LocalIp == KConfig.ClientIp
}

func GetNode(id int64) *Node {
	if KConfig.Id2Node == nil {
		Error("KConfig.Id2Node is not initialized!")
	}
	node, ok := KConfig.Id2Node[id]
	if !ok {
		Error("The node of this ID(%d) does not exist!", id)
	}
	return node
}

func (replica *Replica) GetIndex() int {
	for idx, id := range KConfig.PeerIds {
		if replica.node.id == id {
			return idx
		}
	}
	return -1
}
