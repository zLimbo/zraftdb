package test

import (
	"fmt"
	"testing"
	"time"
	"zpbft/pbft"
)

//var priKey, pubKey = pbft.ReadKeyPair("../certs/1001100119301")

func TestDigest(t *testing.T) {

	req := &pbft.Message{
		MsgType:   pbft.MtRequest,
		Seq:       1,
		NodeId:    0,
		Timestamp: time.Now().UnixNano(),
		Txs:       &pbft.BatchTx{},
	}
	pbft.SignRequest(req, priKey)

	digest := pbft.Sha256Digest(req)

	fmt.Println("digest size:", len(digest))
}
