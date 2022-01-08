package pbft

import (
	"strconv"
)

type Node struct {
	id      int64
	ip      string
	port    int
	priKey  []byte
	pubKey  []byte
	connMgr *ConnMgr
}

func NewNode(ip string, port int, priKey, pubKey []byte) *Node {
	id := GetId(ip, port)
	node := &Node{id, ip, port, priKey, pubKey, nil}
	node.connMgr = NewConnMgr(node)
	return node
}

func (node *Node) GetAddr() string {
	return node.ip + ":" + strconv.Itoa(node.port)
}

//func (node *Node) getDialAddr() string {
//	if node.dialIdx == 100 {
//		Panic("node.dailIdx == 100, please check network!")
//	}
//	node.dialIdx++
//	dialPort := node.port + node.dialIdx
//	dialAddr := node.ip + ":" + strconv.Itoa(dialPort)
//	return dialAddr
//}
