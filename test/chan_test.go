package test

import (
	"fmt"
	"testing"
	"time"
)

func TestChan(t *testing.T) {
	flowSize := 10 * 1024 * 1024

	flowCtlChan := make(chan byte, flowSize)

	start := time.Now()
	for i := 0; i < flowSize; i++ {
		flowCtlChan <- 0
	}
	fmt.Println("time:", time.Since(start))
}
