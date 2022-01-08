package main

import (
	"flag"
	"log"
	"runtime"

	"zpbft/pbft"
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", pbft.KConfigFile, "配置文件")
	pbft.InitConfig(configFile)

	if pbft.IsClient() { // 启动客户端
		runtime.GOMAXPROCS(runtime.NumCPU())
		client := pbft.NewClient()
		client.Start()

	} else {
		var processIdx int
		flag.IntVar(&processIdx, "i", 1, "进程号")
		flag.Parse()

		runtime.GOMAXPROCS(1)
		nodeId := pbft.GetId(pbft.KConfig.LocalIp, pbft.KConfig.PortBase+processIdx)
		replica := pbft.NewReplica(nodeId)
		replica.Start()
	}
}
