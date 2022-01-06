package test

import (
	"fmt"
	"testing"
)

func TestBuf(t *testing.T) {
	bufChan := make(chan []byte, 1)

	fmt.Println(cap(bufChan))

	buf := make([]byte, 1e8)

	bufChan <- buf

	buf2 := <-bufChan
	fmt.Println(buf[0])
	buf2[0] = 1
	fmt.Println(buf[0])
}
