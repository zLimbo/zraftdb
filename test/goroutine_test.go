package test

import (
	"fmt"
	"runtime"
	"testing"
)

func TestGoroutine(t *testing.T) {

	runtime.GOMAXPROCS(1)
	for i := 0; i < 10; i++ {
		i1 := i
		go func() {
			sum := 0
			for j := 0; j < 10e10; j++ {
				sum += 1
			}
			fmt.Println("go", i1, ":", sum)
		}()
	}

	select {}
}
