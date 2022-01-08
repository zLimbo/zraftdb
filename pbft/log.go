package pbft

import (
	"fmt"
	"log"
	"math"
	"runtime"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

func Info(format string, v ...interface{}) {
	log.Output(2, fmt.Sprintf("\033[32m"+format+"\033[0m", v...))
}

func Warn(format string, v ...interface{}) {
	log.Output(2, fmt.Sprintf("\033[33m"+format+"\033[0m", v...))
}

func Panic(format string, v ...interface{}) {
	s := fmt.Sprintf("\033[31m"+format+"\033[0m", v...)
	log.Output(2, s)
	panic(s)
}

func LogStageStart(msg string) {
	Info("\033[031m[%s START] timestamp:%d\033[0m\n", msg, time.Now().Unix())
}

func LogStageEnd(msg string) {
	Info("\033[032m[%s END] timestamp:%d\033[0m\n", msg, time.Now().Unix())
}

func ToSecond(td time.Duration) float64 {
	return float64(td.Nanoseconds()) / math.Pow10(9)
}

func PrintTime(info string, td time.Duration) {
	Info("%s: %0.2fs\n", info, ToSecond(td))
}

func (pbft *Replica) showBatchTime() {
	Info("\033[34m\n[Batch Time seq=%d]\033[0m\n", pbft.batchSeq)

	Info("count1: %d, count2: %d, count3: %d\n", pbft.curBatch.counts[1], pbft.curBatch.counts[2], pbft.curBatch.counts[3])
	Info("[sum]")
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
		Info("\n+++ [avg] batch num:", len(pbft.batchPool), "pbft.batchTimeOkNum:", stat.batchTimeOkNum)
		PrintTime("time0", stat.time0/time.Duration(stat.batchTimeOkNum))
		PrintTime("time1", stat.time1/time.Duration(stat.batchTimeOkNum))
		PrintTime("time2", stat.time2/time.Duration(stat.batchTimeOkNum))
		PrintTime("time3", stat.time3/time.Duration(stat.batchTimeOkNum))
	}

	if stat.execTimeCnt > 0 {
		Info("+++ execTime")
		Info("execTimeCnt", stat.execTimeCnt)
		PrintTime("execTimeSum", stat.execTimeSum)
		PrintTime("execTimeAvg", stat.execTimeSum/stat.execTimeCnt)
	}

}

func (pbft *Replica) showTime(msgCert *MsgCert) {
	Info("\033[34m\n[MsgCert Time seq=%d]\033[0m\n", msgCert.Seq)
	// Info("request:\t%0.6fs\n", float64(msgCert.RequestTime)/math.Pow10(9))
	// Info("pre-prepare:\t%0.6fs\n", float64(msgCert.PrePrepareTime)/math.Pow10(9))
	// Info("prepare:\t%0.6fs\n", float64(msgCert.PrepareTime)/math.Pow10(9))
	// Info("commit:\t\t%0.6fs\n", float64(msgCert.CommitTime)/math.Pow10(9))
	// Info("cert time:\t%0.6fs\n", float64(msgCert.Time)/math.Pow10(9))
	// Info("cert time2:\t%0.6fs\n", float64(msgCert.Time2)/math.Pow10(9))
	stat := pbft.stat
	Info("\nnode times:")
	for idx, time := range stat.times {
		Info("%d: %0.6fs\n", idx, float64(time)/math.Pow10(9))
	}

	Info("")
	if msgCert.Seq/10000 == pbft.node.id {
		stat.time3pcSum += msgCert.Time
		stat.count3pc++
	} else {
		stat.time2pcSum += msgCert.Time
		stat.count2pc++
		stat.prepareTime += msgCert.PrepareTime
		stat.commitTime += msgCert.CommitTime
	}

	Info("\033[34m\n[Avg Time]\033[0m\n")
	Info("node time info:")
	Info("count3pc:", stat.count3pc, "count2pc:", stat.count2pc)
	Info("stat.time3pcSum:\t%0.6fs\n", float64(stat.time3pcSum)/math.Pow10(9))
	Info("stat.time2pcSum:\t%0.6fs\n", float64(stat.time2pcSum)/math.Pow10(9))
	Info("stat.prepareTime:\t%0.6fs\n", float64(stat.prepareTime)/math.Pow10(9))
	Info("stat.commitTime:\t%0.6fs\n", float64(stat.commitTime)/math.Pow10(9))
	Info("stat.prepareTime + stat.commitTime:\t%0.6fs\n", float64(stat.prepareTime+stat.commitTime)/math.Pow10(9))

	Info("Avg time info:")
	if stat.count3pc == 0 || stat.count2pc == 0 {
		return
	}
	avgTime3pc := float64(stat.time3pcSum) / float64(stat.count3pc)
	avgTime2pc := float64(stat.time2pcSum) / float64(stat.count2pc)
	avgTimePrePrepare := avgTime3pc - avgTime2pc
	avgTimePrepare := float64(stat.prepareTime) / float64(stat.count2pc)
	avgTimeCommit := float64(stat.commitTime) / float64(stat.count2pc)

	Info("avgTime3pc:\t%0.6fs\n", avgTime3pc/math.Pow10(9))
	Info("avgTime2pc:\t%0.6fs\n", avgTime2pc/math.Pow10(9))
	Info("avgTimePrePrepare:\t%0.6fs\n", avgTimePrePrepare/math.Pow10(9))
	Info("avgTimePrepare:\t%0.6fs\n", avgTimePrepare/math.Pow10(9))
	Info("avgTimeCommit:\t%0.6fs\n", avgTimeCommit/math.Pow10(9))

	Info("\n+++ chan len ++++")
	Info("recvChan:", len(recvChan))
	Info("connectChan:", len(connectChan))
	Info("pbft.node.connMgr.recvChan:", len(pbft.node.connMgr.recvChan))
	Info("pbft.node.connMgr.sendChan:", len(pbft.node.connMgr.sendChan))

}

func (pbft *Replica) status() {
	for {
		time.Sleep(time.Second * 5)

		Info("\n+++ batch seq:", pbft.batchSeq)
		Info("[req=%d, pre-prepare=%d, prepare=%d, commit=%d, reply=%d]\n",
			pbft.stat.requestNum,
			pbft.stat.prePrepareNum,
			pbft.stat.prepareNum,
			pbft.stat.commitNum,
			pbft.stat.replyNum)
		Info("[recv pre-prepare num:", pbft.curBatch.prePrepareMsgNum, "]")
		Info("send error num:", sendErrorCount)
		Info("goroutine num:", runtime.NumGoroutine())

		Info("\n+++ conn status")
		for id, node := range KConfig.Id2Node {
			if id == pbft.node.id {
				continue
			}
			Info(node.GetAddr() + " connect")
			if node.connMgr.getTcpConn() != nil {
				Info(" success")
			} else {
				Info(" failed")
			}
		}
		log.Print("client " + KConfig.ClientNode.GetAddr() + " connect")
		if KConfig.ClientNode.connMgr.getTcpConn() != nil {
			Info(" success")
		} else {
			Info(" failed")
		}

		stat := pbft.stat
		Info("\n+++ [avg] batch num:", len(pbft.batchPool), "pbft.batchTimeOkNum:", stat.batchTimeOkNum)
		if stat.batchTimeOkNum > 0 {
			Info("time1: %0.2f\n", stat.time1/time.Duration(stat.batchTimeOkNum))
			Info("time2: %0.2f\n", stat.time2/time.Duration(stat.batchTimeOkNum))
			Info("time3: %0.2f\n", stat.time3/time.Duration(stat.batchTimeOkNum))
		}

		Info("\n+++ execTime")
		Info("execTimeCnt:", int64(stat.execTimeCnt))
		if stat.execTimeCnt > 0 {
			PrintTime("execTimeSum", stat.execTimeSum)
			PrintTime("execTimeAvg", stat.execTimeSum/stat.execTimeCnt)
		}

		Info("\n+++ signTime")
		Info("signTimeCnt:", int64(stat.signTimeCnt))
		if stat.signTimeCnt > 0 {
			PrintTime("signTimeSum", stat.signTimeSum)
			PrintTime("signTimeAvg", stat.signTimeSum/stat.signTimeCnt)
		}

		Info("\n+++ verifyTime")
		Info("verifyTimeCnt:", int64(stat.verifyTimeCnt))
		if stat.verifyTimeCnt > 0 {
			PrintTime("verifyTimeSum", stat.verifyTimeSum)
			PrintTime("verifyTimeAvg", stat.verifyTimeSum/stat.verifyTimeCnt)
		}

		Info("\033[34m\n[Avg Time]\033[0m\n")
		Info("node time info:")
		Info("count3pc:", stat.count3pc, "count2pc:", stat.count2pc)
		Info("stat.time3pcSum:\t%0.6fs\n", float64(stat.time3pcSum)/math.Pow10(9))
		Info("stat.time2pcSum:\t%0.6fs\n", float64(stat.time2pcSum)/math.Pow10(9))
		Info("stat.prepareTime:\t%0.6fs\n", float64(stat.prepareTime)/math.Pow10(9))
		Info("stat.commitTime:\t%0.6fs\n", float64(stat.commitTime)/math.Pow10(9))
		Info("stat.prepareTime + stat.commitTime:\t%0.6fs\n", float64(stat.prepareTime+stat.commitTime)/math.Pow10(9))

		Info("Avg time info:")
		if stat.count3pc == 0 || stat.count2pc == 0 {
			continue
		}
		avgTime3pc := float64(stat.time3pcSum) / float64(stat.count3pc)
		avgTime2pc := float64(stat.time2pcSum) / float64(stat.count2pc)
		avgTimePrePrepare := avgTime3pc - avgTime2pc
		avgTimePrepare := float64(stat.prepareTime) / float64(stat.count2pc)
		avgTimeCommit := float64(stat.commitTime) / float64(stat.count2pc)

		Info("avgTime3pc:\t%0.6fs\n", avgTime3pc/math.Pow10(9))
		Info("avgTime2pc:\t%0.6fs\n", avgTime2pc/math.Pow10(9))
		Info("avgTimePrePrepare:\t%0.6fs\n", avgTimePrePrepare/math.Pow10(9))
		Info("avgTimePrepare:\t%0.6fs\n", avgTimePrepare/math.Pow10(9))
		Info("avgTimeCommit:\t%0.6fs\n", avgTimeCommit/math.Pow10(9))

		Info("\n+++ chan len ++++")
		Info("recvChan:", len(recvChan))
		Info("connectChan:", len(connectChan))
		Info("pbft.node.connMgr.recvChan:", len(pbft.node.connMgr.recvChan))
		Info("pbft.node.connMgr.sendChan:", len(pbft.node.connMgr.sendChan))

	}
}
