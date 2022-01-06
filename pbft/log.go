package pbft

import (
	"fmt"
	"math"
	"runtime"
	"time"
)

func LogStageStart(msg string) {
	fmt.Printf("\033[031m[%s START] timestamp:%d\033[0m\n", msg, time.Now().Unix())
}

func LogStageEnd(msg string) {
	fmt.Printf("\033[032m[%s END] timestamp:%d\033[0m\n", msg, time.Now().Unix())
}

func ToSecond(td time.Duration) float64 {
	return float64(td.Nanoseconds()) / math.Pow10(9)
}

func PrintTime(info string, td time.Duration) {
	fmt.Printf("%s: %0.2fs\n", info, ToSecond(td))
}

func (pbft *Pbft) showBatchTime() {
	fmt.Printf("\033[34m\n[Batch Time seq=%d]\033[0m\n", pbft.batchSeq)

	fmt.Printf("count1: %d, count2: %d, count3: %d\n", pbft.curBatch.counts[1], pbft.curBatch.counts[2], pbft.curBatch.counts[3])
	fmt.Println("[sum]")
	times := [4]time.Duration{}
	for i := 0; i < 4; i++ {
		times[i] = pbft.curBatch.times[i+1].Sub(pbft.curBatch.times[i])
	}

	PrintTime("time0", times[0])
	PrintTime("time1", times[1])
	PrintTime("time2", times[2])
	PrintTime("time3", times[3])

	stat := pbft.stat
	if times[0] < 1000 && times[1] < 1000 && times[2] < 1000 && times[3] < 1000 {
		stat.time0 += times[0]
		stat.time1 += times[1]
		stat.time2 += times[2]
		stat.time3 += times[3]
		stat.batchTimeOkNum++
	}

	if stat.batchTimeOkNum > 0 {
		fmt.Println("\n+++ [avg] batch num:", len(pbft.batchPool), "pbft.batchTimeOkNum:", stat.batchTimeOkNum)
		PrintTime("time0", stat.time0/time.Duration(stat.batchTimeOkNum))
		PrintTime("time1", stat.time1/time.Duration(stat.batchTimeOkNum))
		PrintTime("time2", stat.time2/time.Duration(stat.batchTimeOkNum))
		PrintTime("time3", stat.time3/time.Duration(stat.batchTimeOkNum))
	}

	if stat.execTimeCnt > 0 {
		fmt.Println("+++ execTime")
		fmt.Println("execTimeCnt", stat.execTimeCnt)
		PrintTime("execTimeSum", stat.execTimeSum)
		PrintTime("execTimeAvg", stat.execTimeSum/stat.execTimeCnt)
	}

	fmt.Println()
}

func (pbft *Pbft) showTime(msgCert *MsgCert) {
	fmt.Printf("\033[34m\n[MsgCert Time seq=%d]\033[0m\n", msgCert.Seq)
	// fmt.Printf("request:\t%0.6fs\n", float64(msgCert.RequestTime)/math.Pow10(9))
	// fmt.Printf("pre-prepare:\t%0.6fs\n", float64(msgCert.PrePrepareTime)/math.Pow10(9))
	// fmt.Printf("prepare:\t%0.6fs\n", float64(msgCert.PrepareTime)/math.Pow10(9))
	// fmt.Printf("commit:\t\t%0.6fs\n", float64(msgCert.CommitTime)/math.Pow10(9))
	// fmt.Printf("cert time:\t%0.6fs\n", float64(msgCert.Time)/math.Pow10(9))
	// fmt.Printf("cert time2:\t%0.6fs\n", float64(msgCert.Time2)/math.Pow10(9))
	stat := pbft.stat
	fmt.Println("\nnode times:")
	for idx, time := range stat.times {
		fmt.Printf("%d: %0.6fs\n", idx, float64(time)/math.Pow10(9))
	}
	fmt.Println()

	fmt.Println("")
	if msgCert.Seq/10000 == pbft.node.id {
		stat.time3pcSum += msgCert.Time
		stat.count3pc++
	} else {
		stat.time2pcSum += msgCert.Time
		stat.count2pc++
		stat.prepareTime += msgCert.PrepareTime
		stat.commitTime += msgCert.CommitTime
	}

	fmt.Printf("\033[34m\n[Avg Time]\033[0m\n")
	fmt.Println("node time info:")
	fmt.Println("count3pc:", stat.count3pc, "count2pc:", stat.count2pc)
	fmt.Printf("stat.time3pcSum:\t%0.6fs\n", float64(stat.time3pcSum)/math.Pow10(9))
	fmt.Printf("stat.time2pcSum:\t%0.6fs\n", float64(stat.time2pcSum)/math.Pow10(9))
	fmt.Printf("stat.prepareTime:\t%0.6fs\n", float64(stat.prepareTime)/math.Pow10(9))
	fmt.Printf("stat.commitTime:\t%0.6fs\n", float64(stat.commitTime)/math.Pow10(9))
	fmt.Printf("stat.prepareTime + stat.commitTime:\t%0.6fs\n", float64(stat.prepareTime+stat.commitTime)/math.Pow10(9))

	fmt.Println("Avg time info:")
	if stat.count3pc == 0 || stat.count2pc == 0 {
		return
	}
	avgTime3pc := float64(stat.time3pcSum) / float64(stat.count3pc)
	avgTime2pc := float64(stat.time2pcSum) / float64(stat.count2pc)
	avgTimePrePrepare := avgTime3pc - avgTime2pc
	avgTimePrepare := float64(stat.prepareTime) / float64(stat.count2pc)
	avgTimeCommit := float64(stat.commitTime) / float64(stat.count2pc)

	fmt.Printf("avgTime3pc:\t%0.6fs\n", avgTime3pc/math.Pow10(9))
	fmt.Printf("avgTime2pc:\t%0.6fs\n", avgTime2pc/math.Pow10(9))
	fmt.Printf("avgTimePrePrepare:\t%0.6fs\n", avgTimePrePrepare/math.Pow10(9))
	fmt.Printf("avgTimePrepare:\t%0.6fs\n", avgTimePrepare/math.Pow10(9))
	fmt.Printf("avgTimeCommit:\t%0.6fs\n", avgTimeCommit/math.Pow10(9))

	fmt.Println("\n+++ chan len ++++")
	fmt.Println("recvChan:", len(recvChan))
	fmt.Println("connectChan:", len(connectChan))
	fmt.Println("pbft.node.netMgr.recvChan:", len(pbft.node.netMgr.recvChan))
	fmt.Println("pbft.node.netMgr.sendChan:", len(pbft.node.netMgr.sendChan))
	fmt.Println("flowCtlChan:", len(flowCtlChan), "/", cap(flowCtlChan))
	fmt.Println("flowCtlTime:", flowCtlTime)
	fmt.Println()

}

func (pbft *Pbft) status() {
	for {
		time.Sleep(time.Second * 5)

		fmt.Println("\n+++ batch seq:", pbft.batchSeq)
		fmt.Printf("[req=%d, pre-prepare=%d, prepare=%d, commit=%d, reply=%d]\n",
			pbft.stat.requestNum,
			pbft.stat.prePrepareNum,
			pbft.stat.prepareNum,
			pbft.stat.commitNum,
			pbft.stat.replyNum)
		fmt.Println("[recv pre-prepare num:", pbft.curBatch.prePrepareMsgNum, "]")
		fmt.Println("send error num:", sendErrorCount)
		fmt.Println("goroutine num:", runtime.NumGoroutine())

		fmt.Println("\n+++ conn status")
		for id, node := range NodeTable {
			if id == pbft.node.id {
				continue
			}
			fmt.Printf(node.getAddr() + " connect")
			if node.netMgr.getTcpConn() != nil {
				fmt.Println(" success")
			} else {
				fmt.Println(" failed")
			}
		}
		fmt.Print("client " + ClientNode.getAddr() + " connect")
		if ClientNode.netMgr.getTcpConn() != nil {
			fmt.Println(" success")
		} else {
			fmt.Println(" failed")
		}

		stat := pbft.stat
		fmt.Println("\n+++ [avg] batch num:", len(pbft.batchPool), "pbft.batchTimeOkNum:", stat.batchTimeOkNum)
		if stat.batchTimeOkNum > 0 {
			fmt.Printf("time1: %0.2f\n", stat.time1/time.Duration(stat.batchTimeOkNum))
			fmt.Printf("time2: %0.2f\n", stat.time2/time.Duration(stat.batchTimeOkNum))
			fmt.Printf("time3: %0.2f\n", stat.time3/time.Duration(stat.batchTimeOkNum))
		}

		fmt.Println("\n+++ execTime")
		fmt.Println("execTimeCnt:", int64(stat.execTimeCnt))
		if stat.execTimeCnt > 0 {
			PrintTime("execTimeSum", stat.execTimeSum)
			PrintTime("execTimeAvg", stat.execTimeSum/stat.execTimeCnt)
		}

		fmt.Println("\n+++ signTime")
		fmt.Println("signTimeCnt:", int64(stat.signTimeCnt))
		if stat.signTimeCnt > 0 {
			PrintTime("signTimeSum", stat.signTimeSum)
			PrintTime("signTimeAvg", stat.signTimeSum/stat.signTimeCnt)
		}

		fmt.Println("\n+++ verifyTime")
		fmt.Println("verifyTimeCnt:", int64(stat.verifyTimeCnt))
		if stat.verifyTimeCnt > 0 {
			PrintTime("verifyTimeSum", stat.verifyTimeSum)
			PrintTime("verifyTimeAvg", stat.verifyTimeSum/stat.verifyTimeCnt)
		}

		fmt.Printf("\033[34m\n[Avg Time]\033[0m\n")
		fmt.Println("node time info:")
		fmt.Println("count3pc:", stat.count3pc, "count2pc:", stat.count2pc)
		fmt.Printf("stat.time3pcSum:\t%0.6fs\n", float64(stat.time3pcSum)/math.Pow10(9))
		fmt.Printf("stat.time2pcSum:\t%0.6fs\n", float64(stat.time2pcSum)/math.Pow10(9))
		fmt.Printf("stat.prepareTime:\t%0.6fs\n", float64(stat.prepareTime)/math.Pow10(9))
		fmt.Printf("stat.commitTime:\t%0.6fs\n", float64(stat.commitTime)/math.Pow10(9))
		fmt.Printf("stat.prepareTime + stat.commitTime:\t%0.6fs\n", float64(stat.prepareTime+stat.commitTime)/math.Pow10(9))

		fmt.Println("Avg time info:")
		if stat.count3pc == 0 || stat.count2pc == 0 {
			continue
		}
		avgTime3pc := float64(stat.time3pcSum) / float64(stat.count3pc)
		avgTime2pc := float64(stat.time2pcSum) / float64(stat.count2pc)
		avgTimePrePrepare := avgTime3pc - avgTime2pc
		avgTimePrepare := float64(stat.prepareTime) / float64(stat.count2pc)
		avgTimeCommit := float64(stat.commitTime) / float64(stat.count2pc)

		fmt.Printf("avgTime3pc:\t%0.6fs\n", avgTime3pc/math.Pow10(9))
		fmt.Printf("avgTime2pc:\t%0.6fs\n", avgTime2pc/math.Pow10(9))
		fmt.Printf("avgTimePrePrepare:\t%0.6fs\n", avgTimePrePrepare/math.Pow10(9))
		fmt.Printf("avgTimePrepare:\t%0.6fs\n", avgTimePrepare/math.Pow10(9))
		fmt.Printf("avgTimeCommit:\t%0.6fs\n", avgTimeCommit/math.Pow10(9))

		fmt.Println("\n+++ chan len ++++")
		fmt.Println("recvChan:", len(recvChan))
		fmt.Println("connectChan:", len(connectChan))
		fmt.Println("pbft.node.netMgr.recvChan:", len(pbft.node.netMgr.recvChan))
		fmt.Println("pbft.node.netMgr.sendChan:", len(pbft.node.netMgr.sendChan))
		fmt.Println("flowCtlChan:", len(flowCtlChan), "/", cap(flowCtlChan))
		fmt.Println("flowCtlTime:", flowCtlTime)
		fmt.Println()
	}
}
