package pbft

import (
	"fmt"
)

type MessageType int

const (
	MtRequest MessageType = iota
	MtPrePrepare
	MtPrepare
	MtCommit
	MtReply

	MtProposal
)

type Message struct {
	MsgType   MessageType `json:"msgType"`
	Seq       int64       `json:"seq"`
	NodeId    int64       `json:"nodeId"`
	Timestamp int64       `json:"timestamp"`

	Tx  []byte
	Req *Message `json:"req"`
}

type SignMessage struct {
	Msg    *Message `json:"msg"`
	Sign   []byte   `json:"sign"`
	NodeId int64    `json:"nodeId"`
}

type SendStatus int

const (
	NoSend SendStatus = iota
	WaitSend
	HasSend
)

type MsgCert struct {
	Seq        int64
	Digest     []byte
	Req        *Message
	PrePrepare *Message
	Prepares   []*Message
	Commits    []*Message

	SendPrePrepare SendStatus
	SendPrepare    SendStatus
	SendCommit     SendStatus
	SendReply      SendStatus

	// RequestTime    int64
	// PrePrepareTime int64
	// PrepareTime    int64
	// CommitTime     int64
}

func NewMsgCert() *MsgCert {
	return &MsgCert{
		Prepares:       make([]*Message, 0),
		Commits:        make([]*Message, 0),
		SendPrePrepare: NoSend,
		SendPrepare:    NoSend,
		SendCommit:     NoSend,
		SendReply:      NoSend,
	}
}

type ReplyCert struct {
	Seq        int64
	CanApply   bool
	Time       int64
	RequestMsg *Message
	Replys     []*Message
}

func NewReplyCert(seq int64) *ReplyCert {
	return &ReplyCert{
		Seq:      seq,
		Replys:   make([]*Message, 0),
		CanApply: false,
	}
}

func GetMsgId(msg *Message) string {
	return fmt.Sprintf("%d%d%d", msg.Seq, msg.NodeId, msg.MsgType)
}
