package main

import "strconv"

type Node struct {
	id      int64
	ip      string
	port    int
	addr    string
	priKey  []byte
	pubKey  []byte
}

func NewNode(ip string, port int, priKey, pubKey []byte) *Node {
	id := GetId(ip, port)
	node := &Node{id, ip, port, ip + ":" + strconv.Itoa(port), priKey, pubKey}
	return node
}