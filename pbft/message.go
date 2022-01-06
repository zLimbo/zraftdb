package pbft

type MessageType int

const (
	MtRequest MessageType = iota
	MtPrePrepare
	MtPrepare
	MtCommit
	MtReply

	MtProposal
)

type BatchTx [BatchTxNum][TxSize]byte

type Message struct {
	MsgType   MessageType `json:"msgType"`
	Seq       int64       `json:"seq"`
	NodeId    int64       `json:"nodeId"`
	Timestamp int64       `json:"timestamp"`

	Txs     *BatchTx `json:"txs"`
	TxSigns [][]byte `json:"txSigns"`
	Req     *Message `json:"req"`
}

type SignMessage struct {
	Msg  *Message `json:"msg"`
	Sign []byte   `json:"sign"`
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

	RequestTime    int64
	PrePrepareTime int64
	PrepareTime    int64
	CommitTime     int64
	Time           int64
	Time2          int64
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
