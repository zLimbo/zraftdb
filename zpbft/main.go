package main

import (
	"flag"
	"runtime"
)

func main() {
	InitConfig()

	if IsClient() { // 启动客户端

		Info("start client...")

		var clientNum int
		flag.IntVar(&clientNum, "clientNum", 1, "客户端并发数目")
		flag.Parse()
		Info("clientNum: %d", clientNum)
		runtime.GOMAXPROCS(clientNum)

		RunClient(clientNum)

	} else {
		var processIdx int
		flag.IntVar(&processIdx, "i", 1, "进程号")
		flag.Parse()

		Info("start server...")
		runtime.GOMAXPROCS(KConfig.GoMaxProcs)
		nodeId := GetId(KConfig.LocalIp, KConfig.PortBase+processIdx)

		RunServer(nodeId)

	}
}
