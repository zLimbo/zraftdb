package main

import (
	"flag"
	"runtime"

	"zpbft/pbft"
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", pbft.KConfigFile, "配置文件")
	pbft.InitConfig(configFile)

	if pbft.IsClient() { // 启动客户端

		pbft.Info("start client...")
		runtime.GOMAXPROCS(runtime.NumCPU())
		client := pbft.NewClient()
		client.Start()

	} else {
		var processIdx int
		flag.IntVar(&processIdx, "i", 1, "进程号")
		flag.Parse()

		pbft.Info("start server...")
		runtime.GOMAXPROCS(pbft.KConfig.GoMaxProcs)
		nodeId := pbft.GetId(pbft.KConfig.LocalIp, pbft.KConfig.PortBase+processIdx)
		replica := pbft.NewReplica(nodeId)
		replica.Start()

	}
}
