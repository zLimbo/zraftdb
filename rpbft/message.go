package main

import "time"

type Request struct {
	Seq      int64
	FromId   int64
	Date     []byte
	Digest   []byte
	Sigature []byte
}

type Log struct {
	req *Request
}

type Stage = int32

const (
	PrepareStage = iota
	CommitStage
	ReplyStage
)

type MsgCert struct {
	req           *Request
	digest        []byte
	prepareBallot int32
	commitBallot  int32
	stage         Stage
}

type ReqCert struct {
	seq        int64
	digest     []byte
	start      time.Time
	replyCount int

}
