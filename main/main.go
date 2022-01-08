package main

import (
	"log"
	"os"
	"runtime"
	"strconv"

	"zpbft/pbft"
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
	// runtime.GOMAXPROCS(1)
}

// const kConfigFile = "config/config.json"

func main() {

	// args: 参与ip个数（按node.txt顺序） 本ip进程数 本ip第几个结点 boost数 本结点请求数 请求开始延时
	// var ipNum, processNum, processIdx, boostNum, reqNum, boostDelay int

	ipNum, _ := strconv.Atoi(os.Args[1])
	processNum, _ := strconv.Atoi(os.Args[2])
	processIdx, _ := strconv.Atoi(os.Args[3])

	boostNum := ipNum * processIdx
	reqNum, boostDelay := 0, pbft.BoostDelay
	if len(os.Args) > 4 {
		boostNum, _ = strconv.Atoi(os.Args[4])
		if len(os.Args) > 5 {
			reqNum, _ = strconv.Atoi(os.Args[5])
			if len(os.Args) > 6 {
				boostDelay, _ = strconv.Atoi(os.Args[6])
			}
		}
	}

	pbft.BoostNum = boostNum
	log.Printf("# boostNum=%d, reqNum=%d, boostDelay=%d\n", boostNum, reqNum, boostDelay)

	pbft.InitConfig(ipNum, processNum)

	localIp := pbft.GetLocalIp()
	if localIp == pbft.KConfig.ClientIp {
		runClient()
		return
	} else {
		runServer(processIdx, reqNum, boostDelay)
	}

	
	
}

func runServer(processIdx, reqNum, boostDelay int) {
	ip := pbft.GetLocalIp()
	port := 10000 + processIdx
	nodeId := pbft.GetId(ip, port)

	log.Println("## node ip:", ip)
	log.Println("## node port:", port)
	log.Println("## node id:", nodeId)

	server := pbft.NewPbft(nodeId)
	server.Start(reqNum, boostDelay)
}

func runClient() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client := pbft.NewClient()
	client.Start()
}
