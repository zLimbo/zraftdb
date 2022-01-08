package pbft

import (
	"log"
	"math"
	"runtime"
	"time"
)

func LogStageStart(msg string) {
	log.Printf("\033[031m[%s START] timestamp:%d\033[0m\n", msg, time.Now().Unix())
}

func LogStageEnd(msg string) {
	log.Printf("\033[032m[%s END] timestamp:%d\033[0m\n", msg, time.Now().Unix())
}

func ToSecond(td time.Duration) float64 {
	return float64(td.Nanoseconds()) / math.Pow10(9)
}

func PrintTime(info string, td time.Duration) {
	log.Printf("%s: %0.2fs\n", info, ToSecond(td))
}

func (pbft *Pbft) showBatchTime() {
	log.Printf("\033[34m\n[Batch Time seq=%d]\033[0m\n", pbft.batchSeq)

	log.Printf("count1: %d, count2: %d, count3: %d\n", pbft.curBatch.counts[1], pbft.curBatch.counts[2], pbft.curBatch.counts[3])
	log.Println("[sum]")
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
		log.Println("\n+++ [avg] batch num:", len(pbft.batchPool), "pbft.batchTimeOkNum:", stat.batchTimeOkNum)
		PrintTime("time0", stat.time0/time.Duration(stat.batchTimeOkNum))
		PrintTime("time1", stat.time1/time.Duration(stat.batchTimeOkNum))
		PrintTime("time2", stat.time2/time.Duration(stat.batchTimeOkNum))
		PrintTime("time3", stat.time3/time.Duration(stat.batchTimeOkNum))
	}

	if stat.execTimeCnt > 0 {
		log.Println("+++ execTime")
		log.Println("execTimeCnt", stat.execTimeCnt)
		PrintTime("execTimeSum", stat.execTimeSum)
		PrintTime("execTimeAvg", stat.execTimeSum/stat.execTimeCnt)
	}

	log.Println()
}

func (pbft *Pbft) showTime(msgCert *MsgCert) {
	log.Printf("\033[34m\n[MsgCert Time seq=%d]\033[0m\n", msgCert.Seq)
	// log.Printf("request:\t%0.6fs\n", float64(msgCert.RequestTime)/math.Pow10(9))
	// log.Printf("pre-prepare:\t%0.6fs\n", float64(msgCert.PrePrepareTime)/math.Pow10(9))
	// log.Printf("prepare:\t%0.6fs\n", float64(msgCert.PrepareTime)/math.Pow10(9))
	// log.Printf("commit:\t\t%0.6fs\n", float64(msgCert.CommitTime)/math.Pow10(9))
	// log.Printf("cert time:\t%0.6fs\n", float64(msgCert.Time)/math.Pow10(9))
	// log.Printf("cert time2:\t%0.6fs\n", float64(msgCert.Time2)/math.Pow10(9))
	stat := pbft.stat
	log.Println("\nnode times:")
	for idx, time := range stat.times {
		log.Printf("%d: %0.6fs\n", idx, float64(time)/math.Pow10(9))
	}
	log.Println()

	log.Println("")
	if msgCert.Seq/10000 == pbft.node.id {
		stat.time3pcSum += msgCert.Time
		stat.count3pc++
	} else {
		stat.time2pcSum += msgCert.Time
		stat.count2pc++
		stat.prepareTime += msgCert.PrepareTime
		stat.commitTime += msgCert.CommitTime
	}

	log.Printf("\033[34m\n[Avg Time]\033[0m\n")
	log.Println("node time info:")
	log.Println("count3pc:", stat.count3pc, "count2pc:", stat.count2pc)
	log.Printf("stat.time3pcSum:\t%0.6fs\n", float64(stat.time3pcSum)/math.Pow10(9))
	log.Printf("stat.time2pcSum:\t%0.6fs\n", float64(stat.time2pcSum)/math.Pow10(9))
	log.Printf("stat.prepareTime:\t%0.6fs\n", float64(stat.prepareTime)/math.Pow10(9))
	log.Printf("stat.commitTime:\t%0.6fs\n", float64(stat.commitTime)/math.Pow10(9))
	log.Printf("stat.prepareTime + stat.commitTime:\t%0.6fs\n", float64(stat.prepareTime+stat.commitTime)/math.Pow10(9))

	log.Println("Avg time info:")
	if stat.count3pc == 0 || stat.count2pc == 0 {
		return
	}
	avgTime3pc := float64(stat.time3pcSum) / float64(stat.count3pc)
	avgTime2pc := float64(stat.time2pcSum) / float64(stat.count2pc)
	avgTimePrePrepare := avgTime3pc - avgTime2pc
	avgTimePrepare := float64(stat.prepareTime) / float64(stat.count2pc)
	avgTimeCommit := float64(stat.commitTime) / float64(stat.count2pc)

	log.Printf("avgTime3pc:\t%0.6fs\n", avgTime3pc/math.Pow10(9))
	log.Printf("avgTime2pc:\t%0.6fs\n", avgTime2pc/math.Pow10(9))
	log.Printf("avgTimePrePrepare:\t%0.6fs\n", avgTimePrePrepare/math.Pow10(9))
	log.Printf("avgTimePrepare:\t%0.6fs\n", avgTimePrepare/math.Pow10(9))
	log.Printf("avgTimeCommit:\t%0.6fs\n", avgTimeCommit/math.Pow10(9))

	log.Println("\n+++ chan len ++++")
	log.Println("recvChan:", len(recvChan))
	log.Println("connectChan:", len(connectChan))
	log.Println("pbft.node.netMgr.recvChan:", len(pbft.node.netMgr.recvChan))
	log.Println("pbft.node.netMgr.sendChan:", len(pbft.node.netMgr.sendChan))
	log.Println("flowCtlChan:", len(flowCtlChan), "/", cap(flowCtlChan))
	log.Println("flowCtlTime:", flowCtlTime)
	log.Println()

}

func (pbft *Pbft) status() {
	for {
		time.Sleep(time.Second * 5)

		log.Println("\n+++ batch seq:", pbft.batchSeq)
		log.Printf("[req=%d, pre-prepare=%d, prepare=%d, commit=%d, reply=%d]\n",
			pbft.stat.requestNum,
			pbft.stat.prePrepareNum,
			pbft.stat.prepareNum,
			pbft.stat.commitNum,
			pbft.stat.replyNum)
		log.Println("[recv pre-prepare num:", pbft.curBatch.prePrepareMsgNum, "]")
		log.Println("send error num:", sendErrorCount)
		log.Println("goroutine num:", runtime.NumGoroutine())

		log.Println("\n+++ conn status")
		for id, node := range NodeTable {
			if id == pbft.node.id {
				continue
			}
			log.Printf(node.GetAddr() + " connect")
			if node.netMgr.getTcpConn() != nil {
				log.Println(" success")
			} else {
				log.Println(" failed")
			}
		}
		log.Print("client " + ClientNode.GetAddr() + " connect")
		if ClientNode.netMgr.getTcpConn() != nil {
			log.Println(" success")
		} else {
			log.Println(" failed")
		}

		stat := pbft.stat
		log.Println("\n+++ [avg] batch num:", len(pbft.batchPool), "pbft.batchTimeOkNum:", stat.batchTimeOkNum)
		if stat.batchTimeOkNum > 0 {
			log.Printf("time1: %0.2f\n", stat.time1/time.Duration(stat.batchTimeOkNum))
			log.Printf("time2: %0.2f\n", stat.time2/time.Duration(stat.batchTimeOkNum))
			log.Printf("time3: %0.2f\n", stat.time3/time.Duration(stat.batchTimeOkNum))
		}

		log.Println("\n+++ execTime")
		log.Println("execTimeCnt:", int64(stat.execTimeCnt))
		if stat.execTimeCnt > 0 {
			PrintTime("execTimeSum", stat.execTimeSum)
			PrintTime("execTimeAvg", stat.execTimeSum/stat.execTimeCnt)
		}

		log.Println("\n+++ signTime")
		log.Println("signTimeCnt:", int64(stat.signTimeCnt))
		if stat.signTimeCnt > 0 {
			PrintTime("signTimeSum", stat.signTimeSum)
			PrintTime("signTimeAvg", stat.signTimeSum/stat.signTimeCnt)
		}

		log.Println("\n+++ verifyTime")
		log.Println("verifyTimeCnt:", int64(stat.verifyTimeCnt))
		if stat.verifyTimeCnt > 0 {
			PrintTime("verifyTimeSum", stat.verifyTimeSum)
			PrintTime("verifyTimeAvg", stat.verifyTimeSum/stat.verifyTimeCnt)
		}

		log.Printf("\033[34m\n[Avg Time]\033[0m\n")
		log.Println("node time info:")
		log.Println("count3pc:", stat.count3pc, "count2pc:", stat.count2pc)
		log.Printf("stat.time3pcSum:\t%0.6fs\n", float64(stat.time3pcSum)/math.Pow10(9))
		log.Printf("stat.time2pcSum:\t%0.6fs\n", float64(stat.time2pcSum)/math.Pow10(9))
		log.Printf("stat.prepareTime:\t%0.6fs\n", float64(stat.prepareTime)/math.Pow10(9))
		log.Printf("stat.commitTime:\t%0.6fs\n", float64(stat.commitTime)/math.Pow10(9))
		log.Printf("stat.prepareTime + stat.commitTime:\t%0.6fs\n", float64(stat.prepareTime+stat.commitTime)/math.Pow10(9))

		log.Println("Avg time info:")
		if stat.count3pc == 0 || stat.count2pc == 0 {
			continue
		}
		avgTime3pc := float64(stat.time3pcSum) / float64(stat.count3pc)
		avgTime2pc := float64(stat.time2pcSum) / float64(stat.count2pc)
		avgTimePrePrepare := avgTime3pc - avgTime2pc
		avgTimePrepare := float64(stat.prepareTime) / float64(stat.count2pc)
		avgTimeCommit := float64(stat.commitTime) / float64(stat.count2pc)

		log.Printf("avgTime3pc:\t%0.6fs\n", avgTime3pc/math.Pow10(9))
		log.Printf("avgTime2pc:\t%0.6fs\n", avgTime2pc/math.Pow10(9))
		log.Printf("avgTimePrePrepare:\t%0.6fs\n", avgTimePrePrepare/math.Pow10(9))
		log.Printf("avgTimePrepare:\t%0.6fs\n", avgTimePrepare/math.Pow10(9))
		log.Printf("avgTimeCommit:\t%0.6fs\n", avgTimeCommit/math.Pow10(9))

		log.Println("\n+++ chan len ++++")
		log.Println("recvChan:", len(recvChan))
		log.Println("connectChan:", len(connectChan))
		log.Println("pbft.node.netMgr.recvChan:", len(pbft.node.netMgr.recvChan))
		log.Println("pbft.node.netMgr.sendChan:", len(pbft.node.netMgr.sendChan))
		log.Println("flowCtlChan:", len(flowCtlChan), "/", cap(flowCtlChan))
		log.Println("flowCtlTime:", flowCtlTime)
		log.Println()
	}
}
