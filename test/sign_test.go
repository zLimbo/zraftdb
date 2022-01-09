package test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"
	"zpbft/pbft"
)

func TestSignTime(t *testing.T) {
	fmt.Println("TestSignTime")
	reqMsg := &pbft.Message{
		MsgType:   pbft.MtRequest,
		Seq:       1,
		NodeId:    0,
		Timestamp: time.Now().UnixNano(),

		Txs: new(pbft.BatchTx),
	}
	reqMsgBytes, _ := json.Marshal(reqMsg)
	fmt.Println("reqMsg len:", float64(len(reqMsgBytes))/float64(pbft.MBSize))

	ppMsg := &pbft.Message{
		MsgType: pbft.MtPrePrepare,
		Seq:     1,
		NodeId:  1,

		Timestamp: time.Now().UnixNano(),
		Req:       reqMsg,
	}
	ppMsgBytes, _ := json.Marshal(ppMsg)
	fmt.Println("ppMsg len:", float64(len(ppMsgBytes))/float64(pbft.MBSize))

	priKey, pubKey := GetKeyPair()

	start := time.Now()
	digest := pbft.Sha256Digest(reqMsg)
	gap1 := time.Since(start)
	start = time.Now()
	sign := pbft.RsaSignWithSha256(digest, priKey)
	gap2 := time.Since(start)
	start = time.Now()
	result := pbft.RsaVerifyWithSha256(digest, sign, pubKey)
	gap3 := time.Since(start)

	fmt.Println("result:", result)
	fmt.Println("digest time:", gap1)
	fmt.Println("sign time:", gap2)
	fmt.Println("verify time:", gap3)
	fmt.Println("sign len:", len(sign))

	t1 := time.Duration(5000)

	fmt.Println("t1:", t1)

}

func TestSignTime2(t *testing.T) {
	fmt.Println("TestSignTime2")
	reqMsg := &pbft.Message{
		MsgType:   pbft.MtRequest,
		Seq:       1,
		NodeId:    0,
		Timestamp: time.Now().UnixNano(),

		Txs: new(pbft.BatchTx),
	}
	reqMsgBytes, _ := json.Marshal(reqMsg)
	fmt.Println("reqMsg size:", float64(len(reqMsgBytes))/float64(pbft.MBSize))

	ppMsg := &pbft.Message{
		MsgType: pbft.MtPrePrepare,
		Seq:     1,
		NodeId:  1,

		Timestamp: time.Now().UnixNano(),
		Req:       reqMsg,
	}
	ppMsgBytes, _ := json.Marshal(ppMsg)
	fmt.Println("ppMsg size:", float64(len(ppMsgBytes))/float64(pbft.MBSize))

	priKey, pubKey := GetKeyPair()

	start := time.Now()
	signMsg := pbft.SignMsg(ppMsg, priKey)
	signMsgBytes, _ := json.Marshal(signMsg)
	fmt.Println("ppSignMsg size:", float64(len(signMsgBytes))/float64(pbft.MBSize))
	fmt.Println("tx num:", len(signMsg.Msg.Req.Txs))
	fmt.Println("signs num:", len(signMsg.Msg.Req.TxSigns))
	gap1 := time.Since(start)
	start = time.Now()
	result := pbft.VerifySignMsg(signMsg, pubKey)
	gap2 := time.Since(start)

	fmt.Println("result:", result)
	fmt.Println("sign time:", gap1)
	fmt.Println("verify time:", gap2)

	prepare := &pbft.Message{
		MsgType: pbft.MtPrepare,
	}

	prepareSign := pbft.SignMsg(prepare, priKey)

	prepareSignBytes, _ := json.Marshal(prepareSign)
	fmt.Println("len:", len(prepareSignBytes))
}

func TestHashTime(t *testing.T) {
	priKey, pubKey := GetKeyPair()
	const threadNum = 4
	runtime.GOMAXPROCS(threadNum)

	size := 400
	msg := make([]byte, size)

	digestTime := time.Duration(0)
	signTime := time.Duration(0)
	verifyTime := time.Duration(0)

	ch := make(chan int, threadNum)

	start := time.Now()
	num := 10000

	now := time.Now()
	for k := 0; k < threadNum; k++ {
		go func(id int) {
			for i := 0; i < num; i++ {

				pbft.Sha256Digest(msg)

			}
			ch <- id
		}(k)
	}
	cnt := 0
	for {
		<-ch
		cnt++
		if cnt == threadNum {
			break
		}
	}
	digestTime += time.Since(now)

	digest := pbft.Sha256Digest(msg)
	now = time.Now()
	for k := 0; k < threadNum; k++ {
		go func(id int) {
			for i := 0; i < num; i++ {
				pbft.RsaSignWithSha256(digest, priKey)
			}
			ch <- id
		}(k)
	}
	cnt = 0
	for {
		<-ch
		cnt++
		if cnt == threadNum {
			break
		}
	}
	signTime += time.Since(now)

	sign := pbft.RsaSignWithSha256(digest, priKey)
	now = time.Now()
	for k := 0; k < threadNum; k++ {
		go func(id int) {
			for i := 0; i < num; i++ {
				pbft.RsaVerifyWithSha256(digest, sign, pubKey)
			}
			ch <- id
		}(k)
	}
	cnt = 0
	for {
		<-ch
		cnt++
		if cnt == threadNum {
			break
		}
	}
	verifyTime += time.Since(now)

	// MB/s
	digestSpeed := float64(size*num*threadNum) / pbft.MBSize / digestTime.Seconds()
	signSpeed := float64(size*num*threadNum) / pbft.MBSize / signTime.Seconds()
	verifySpeed := float64(size*num*threadNum) / pbft.MBSize / verifyTime.Seconds()

	fmt.Println("spend:", time.Since(start))
	fmt.Printf("digestSpend: %0.2f MB/s\n", digestSpeed)
	fmt.Printf("signSpend: %0.2f MB/s\n", signSpeed)
	fmt.Printf("verifySpend: %0.2f MB/s\n", verifySpeed)
}
