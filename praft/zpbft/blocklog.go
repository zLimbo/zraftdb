package main

import "sync"

type BlockLog struct{
	blockLogMutex sync.Mutex
	blockIndex int64
	duplicatedReqs []*duplicatedReqUnit
	txNum int64
	prepareConfirmNodes []int64
	commitConfirmNodes []int64
	committed bool
	prepared bool
	primaryNodeId int64
}

func (blockLog *BlockLog)set(blockIndex int64, prepared bool, committed bool){
    blockLog.blockIndex = blockIndex
    blockLog.committed = false
    blockLog.prepared = false
}

func (blockLog *BlockLog)pushPrepareConfirmNodes(nodeId int64){
	for i := 0; i < len(blockLog.prepareConfirmNodes); i++{
		if nodeId == blockLog.prepareConfirmNodes[i]{
			Debug("Node[%d]'s prepare confirm has existed, ignore it")
			return
		}
	}
	blockLog.prepareConfirmNodes = append(blockLog.prepareConfirmNodes, nodeId)
	return
}

func (blockLog *BlockLog)pushCommitConfirmNodes(nodeId int64){
	for i := 0; i < len(blockLog.commitConfirmNodes); i++{
		if nodeId == blockLog.commitConfirmNodes[i]{
			Debug("Node[%d]'s commit confirm has existed, ignore it")
			return
		}
	}
	blockLog.commitConfirmNodes = append(blockLog.commitConfirmNodes, nodeId)
}

func (blockLog *BlockLog)check(){
	if len(blockLog.prepareConfirmNodes) >= KConfig.FaultNum{
		blockLog.prepared = true
	}
	if len(blockLog.commitConfirmNodes) >= KConfig.FaultNum{
		blockLog.committed = true
	}
}
