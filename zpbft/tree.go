package main

import (
	"net/rpc"
)

type TreeNode struct{
	Id       int64
	ChildNum int64
	Children []*TreeNode
	//orderNum int64      //仅构造树结构排序时用，与树结构和广播模式无关
	//nextNode *treeNode  //仅构造树结构排序时用，与树结构和广播模式无关
}

func makeTree(id2srvCli map[int64]*rpc.Client, childNum int64, localNodeId int64) *TreeNode {
	//var hasChildNodeSet []*treeNode
	var noChildNodeSet  []*TreeNode
	rootNode := TreeNode{
		Id: localNodeId,
		ChildNum: 0,
	}
	var currentNode *TreeNode
	currentNode = &rootNode
	//148045
	//148080
	//148170
	var idleIp []int64
	var busyIp []int64
	var treeIp []int64
	for id, _ := range id2srvCli{
		if id / 100 == 148045 {
			busyIp = append(busyIp, id)
		}else{
			idleIp = append(idleIp, id)
		}
	}
	treeIp = append(idleIp, busyIp...)

	//for _, nodeId := range idleIp{
	//	Debug("idle id = %d", nodeId)
	//}
	//for _, nodeId := range busyIp{
	//	Debug("busy id = %d", nodeId)
	//}
	//for i := 0; i < len(treeIp); i++ {
	//	Debug("tree id = %d", treeIp[i])
	//}
	//for i := 0; i < len(balanceIp); i++ {
	//	Debug("balance id = %d", balanceIp[i])
	//}

	//for _, id := range treeIp{
	//	Debug("id = %d", id)
	//}
	for _, id := range treeIp{
		tN := TreeNode{
			Id: id,
			ChildNum: 0,
		}
		if currentNode.ChildNum < childNum {
			currentNode.Children = append(currentNode.Children, &tN)
			//Debug("tN nodeId = %d", tN.Id)
			currentNode.ChildNum ++
			noChildNodeSet = append(noChildNodeSet, &tN)
		} else {
			if len(noChildNodeSet) >= 1 {
				currentNode = noChildNodeSet[0]
				noChildNodeSet = append(noChildNodeSet[:0], noChildNodeSet[1:]... )
				currentNode.Children = append(currentNode.Children, &tN)
				//Debug("tN nodeId = %d", tN.Id)
				currentNode.ChildNum ++
				noChildNodeSet = append(noChildNodeSet, &tN)
			}
		}
	}

	//for id, _ := range id2srvCli{
    //    tN := TreeNode{
	//		Id: id,
	//		ChildNum: 0,
	//	}
	//	if currentNode.ChildNum < childNum {
	//		currentNode.Children = append(currentNode.Children, &tN)
	//		//Debug("tN nodeId = %d", tN.Id)
	//		currentNode.ChildNum ++
	//		noChildNodeSet = append(noChildNodeSet, &tN)
	//	} else {
	//		if len(noChildNodeSet) >= 1 {
	//			currentNode = noChildNodeSet[0]
	//			noChildNodeSet = append(noChildNodeSet[:0], noChildNodeSet[1:]... )
	//			currentNode.Children = append(currentNode.Children, &tN)
	//			//Debug("tN nodeId = %d", tN.Id)
	//			currentNode.ChildNum ++
	//			noChildNodeSet = append(noChildNodeSet, &tN)
	//		}
	//	}
	//}
	return &rootNode
}
//返回nodeId对应节点的子节点的nodeId
func traverseTree(rootNode *TreeNode, nodeId int64) []int64 {
	var tempNode []*TreeNode
	var childNodeIds []int64
	tempNode = append(tempNode, rootNode)
	for {
		if len(tempNode) == 0 {
			break
		}
		if tempNode[0].Id == nodeId {
			for i := 0; i < int(tempNode[0].ChildNum); i++ {
				childNodeIds = append(childNodeIds, tempNode[0].Children[i].Id)
			}
			return childNodeIds
		}
		for i := 0; i < int(tempNode[0].ChildNum); i++ {
			tempNode = append(tempNode, tempNode[0].Children[i])
		}
		if len(tempNode) >= 2 {
			tempNode = append(tempNode[:0], tempNode[1:]...)
		} else {
			tempNode = nil
		}
	}
	return childNodeIds
}