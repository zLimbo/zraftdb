package test

import (
	"testing"
)

type MessageType int

const (
	mtRequest MessageType = iota
	mtPrePrepare
	mtPrepare
	mtCommit
	mtReply
)

type Message interface {
	MsgType() MessageType
	Seq() int64
	NodeId() int64
	Timestamp() int64
}

// type Message struct {
// 	MsgType   MessageType `json:"msgType"`
// 	Seq       int64       `json:"seq"`
// 	NodeId    int64       `json:"nodeId"`
// 	Timestamp int64       `json:"timestamp"`
// }

// func (msg *Message) showMsg(t *testing.T) {
// 	t.Log(msg)
// }

type SignMessage struct {
	Msg  *Message `json:"msg"`
	Sign []byte   `json:"sign"`
}

type ReqMessage struct {
	Message
	Ext []byte   `json:"ext"`
	Req *Message `json:"req"`
}

func (msg *ReqMessage) showMsg(t *testing.T) {
	t.Log(msg)
}

func TestMessage(t *testing.T) {
	// req := &ReqMessage{}
	// ShowMsg(req, t)

}

func ShowMsg(msg *Message, t *testing.T) {
	t.Log(msg)
}
