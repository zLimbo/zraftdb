package test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"zpbft/pbft"
)

func TestExecTime(t *testing.T) {
	start := time.Now()
	sum := int64(0)
	num := int64(1e10)
	for i := int64(0); i < num; i++ {
		sum += 1
	}
	fmt.Println("Exec result:", sum)
	fmt.Println("Exec time:", time.Since(start))
	fmt.Printf("Exec time: %s\n", time.Since(start))

	start = time.Now()
	req := &pbft.Message{
		MsgType:   pbft.MtRequest,
		Seq:       1,
		NodeId:    0,
		Timestamp: time.Now().UnixNano(),
		Txs:       &pbft.BatchTx{},
	}
	fmt.Println("batch tx gen time:", time.Since(start))
	start = time.Now()
	pbft.SignRequest(req, priKey)
	fmt.Println("sign tx time:", time.Since(start))
	start = time.Now()
	json.Marshal(req)
	fmt.Println("json marsha time:", time.Since(start))
}
