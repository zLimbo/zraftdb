package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"

	"zpbft/pbft"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	runtime.GOMAXPROCS(1)
}

func main() {

	// args: 参与ip个数（按node.txt顺序） 本ip进程数 本ip第几个结点 boost数 本结点请求数 请求开始延时
	// ipNum, processNum, processIdx, boostNum, reqNum, boostDelay int

	ipNum, _ := strconv.Atoi(os.Args[1])
	processNum, _ := strconv.Atoi(os.Args[2])

	pbft.InitNodeAddr(ipNum, processNum)

	processIdx, _ := strconv.Atoi(os.Args[3])

	if processIdx == -1 {
		runClient()
		return
	}

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
	fmt.Printf("# boostNum=%d, reqNum=%d, boostDelay=%d\n", boostNum, reqNum, boostDelay)
	pbft.BoostNum = boostNum
	runServer(processIdx, reqNum, boostDelay)
}

func runServer(processIdx, reqNum, boostDelay int) {
	ip := pbft.GetOutBoundIP()
	port := 10000 + processIdx
	nodeId := pbft.GetId(ip, port)

	fmt.Println("## node ip:", ip)
	fmt.Println("## node port:", port)
	fmt.Println("## node id:", nodeId)

	server := pbft.NewPbft(nodeId)
	server.Start(reqNum, boostDelay)
}

func runClient() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client := pbft.NewClient()
	client.Start()
}
