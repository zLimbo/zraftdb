package main

import (
	"sync"
	"time"
)

type View struct{
	committedInstance map[int64]bool
	committedOrNot bool
	viewNum int64
	startTime time.Time
}

func(view *View) refreshViewCommittedOrNot(instanceNum int64) {
	//Debug("committed instance num = %d, proposer number = %d", int64(len(view.committedInstance)), instanceNum)
	if int64(len(view.committedInstance)) == instanceNum{
		view.committedOrNot = true
		Debug("view %d completed, refreshed", view.viewNum)
	}
}

type LocalView struct{
	views map[int64]View
	currentStableViewHeight int64
	localViewMutex  sync.Mutex
}

func(localView *LocalView) getView(viewNum int64)View{
	view, ok := localView.views[viewNum]
	if !ok{
		view := View{
			committedInstance: make(map[int64]bool),
			committedOrNot: false,
			viewNum: viewNum,
			startTime: time.Now(),
		}
		localView.views[viewNum] = view
		return view
	}
	return view
}

func (localView *LocalView) refreshCurrentStableViewHeight(instanceNum int64) bool{
	for i:= localView.currentStableViewHeight; i < int64(len(localView.views));i++{

		view := localView.views[i]
		view.refreshViewCommittedOrNot(instanceNum)
		if localView.views[i].committedOrNot{
			localView.currentStableViewHeight++
		}else{
			continue
		}

	}
	return true
}