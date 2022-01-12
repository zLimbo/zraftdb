package test

import (
	"fmt"
	"testing"
)

const KRouteNum = 3

func TestRoute(t *testing.T) {

	peerNum := 16
	routeMap := make([][KRouteNum]int, peerNum)

	k := 0
	for i := 0; i < peerNum; i++ {
		for j := 0; j < KRouteNum; j++ {
			if k == i {
				k = (k + 1) % peerNum
			}
			routeMap[i][j] = k
			k = (k + 1) % peerNum
		}
	}

	for i := 0; i < peerNum; i++ {
		fmt.Printf("%2d: %v\n", i, routeMap[i])
	}
}