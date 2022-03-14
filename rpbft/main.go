package main

import (
	"flag"
	"runtime"
)

func main() {
	InitConfig()

	if IsClient() { // 启动客户端

		Info("start client...")
		runtime.GOMAXPROCS(runtime.NumCPU())
		runClient()

	} else {
		var processIdx int
		flag.IntVar(&processIdx, "i", 1, "进程号")
		flag.Parse()

		Info("start server...")
		runtime.GOMAXPROCS(KConfig.GoMaxProcs)
		nodeId := GetId(KConfig.LocalIp, KConfig.PortBase+processIdx)

		runServer(nodeId)

	}
}
