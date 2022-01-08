package pbft

import (
	"strconv"
)

type Node struct {
	id      int64
	ip      string
	port    int
	addr    string
	priKey  []byte
	pubKey  []byte
	connMgr *ConnMgr
}

func NewNode(ip string, port int, priKey, pubKey []byte) *Node {
	id := GetId(ip, port)
	node := &Node{id, ip, port, ip + ":" + strconv.Itoa(port), priKey, pubKey, nil}
	node.connMgr = NewConnMgr(node)
	return node
}
