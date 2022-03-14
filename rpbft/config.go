package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

const (
	MBSize       = 1024 * 1024
	ChanSize     = 10000
	KConfigFile  = "./config/config.json"
	KCertsDir    = "./certs"
	KLocalIpFile = "./config/local_ip.txt"
)

type Config struct {
	PeerIps      []string `json:"PeerIps"`
	ClientIp     string   `json:"ClientIp"`
	IpNum        int      `json:"IpNum"`
	PortBase     int      `json:"PortBase"`
	ProcessNum   int      `json:"ProcessNum"`
	ReqNum       int      `json:"ReqNum"`
	BoostNum     int      `json:"BoostNum"`
	StartDelay   int      `json:"StartDelay"`
	RecvBufSize  int      `json:"RecvBufSize"`
	LogStdout    bool     `json:"LogStdout"`
	LogLevel     LogLevel `json:"LogLevel"`
	GoMaxProcs   int      `json:"GoMaxProcs"`
	BatchTxNum   int      `json:"BatchTxNum"`
	TxSize       int      `json:"TxSize"`
	GossipNum    int      `json:"GossipNum"`
	EnableGossip bool     `json:"EnableGossip"`
	ExecNum      int      `json:"ExecNum"`

	Id2Node    map[int64]*Node
	ClientNode *Node
	PeerIds    []int64
	LocalIp    string
	FalultNum  int
	RouteMap   map[int64][]int64
}

var KConfig Config

func InitConfig() {

	// 读取 json
	jsonBytes, err := ioutil.ReadFile(KConfigFile)
	if err != nil {
		Error("read %s failed.", KConfigFile)
	}
	Debug("config: ", string(jsonBytes))
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
}

func IsClient() bool {
	return KConfig.LocalIp == KConfig.ClientIp
}
