package pbft

import (
	"strconv"
)

type Node struct {
	id     int64
	ip     string
	port   int
	priKey []byte
	pubKey []byte
	netMgr *NetMgr
}

func NewNode(ip string, port int, priKey, pubKey []byte) *Node {
	id := GetId(ip, port)
	node := &Node{id, ip, port, priKey, pubKey, nil}
	node.netMgr = NewNetMgr(node)
	return node
}

func (node *Node) getAddr() string {
	return node.ip + ":" + strconv.Itoa(node.port)
}

//func (node *Node) getDialAddr() string {
//	if node.dialIdx == 100 {
//		log.Panicln("node.dailIdx == 100, please check network!")
//	}
//	node.dialIdx++
//	dialPort := node.port + node.dialIdx
//	dialAddr := node.ip + ":" + strconv.Itoa(dialPort)
//	return dialAddr
//}
