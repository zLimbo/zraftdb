package test

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"zpbft/pbft"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func TestPbft(t *testing.T) {
	// args: 参与ip个数（按node.txt顺序） 本ip进程数 本ip第几个结点 boost数 本结点请求数 请求开始延时
	// ipNum, processNum, processIdx, boostNum, reqNum, boostDelay int
	args := os.Args[3:]
	ipNum, _ := strconv.Atoi(args[1])
	processNum, _ := strconv.Atoi(args[2])

	pbft.InitNodeAddr(ipNum, processNum)

	processIdx, _ := strconv.Atoi(args[3])

	if processIdx == -1 {
		runClient()
		return
	}

	boostNum := ipNum * processIdx
	reqNum, reqSize, boostDelay := 0, 0, 3000
	if len(args) > 4 {
		boostNum, _ = strconv.Atoi(args[4])
		if len(args) > 5 {
			reqNum, _ = strconv.Atoi(args[5])
			if len(args) > 6 {
				boostDelay, _ = strconv.Atoi(args[6])
			}
		}
	}
	fmt.Printf("# boostNum=%d, reqNum=%d, reqSize=%d, boostDelay=%d\n", boostNum, reqNum, reqSize, boostDelay)
	pbft.BoostNum = boostNum
	runServer(processIdx, reqNum, reqSize, boostDelay)
}

func runServer(processIdx, reqNum, reqSize, boostDelay int) {
	ip := pbft.GetOutBoundIP()
	nodeId := pbft.GetId(ip, processIdx)

	fmt.Println("## node ip:", ip)
	fmt.Println("## node id:", nodeId)

	server := pbft.NewPbft(nodeId)
	server.Start(reqNum, boostDelay)
}

func runClient() {
	client := pbft.NewClient()
	client.Start()
}
