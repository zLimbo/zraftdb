package test

import (
	"fmt"
	"testing"
)

func I2Bytes(num, len int) []byte {
	result := make([]byte, len)
	for i := 0; i < len; i++ {
		result[i] = byte(num % 256)
		num /= 256
		if num == 0 {
			break
		}
	}
	return result
}

func Bytes2I(data []byte, len int) int {
	base := 1
	result := 0
	for i := 0; i < len; i++ {
		if data[i] == byte(0) {
			break
		}
		result += int(data[i]) * base
		base *= 256
	}
	return result
}

func TestItoB(t *testing.T) {
	sz := 99999999
	szBytes := I2Bytes(sz, 4)
	for idx, b := range szBytes {
		fmt.Println(idx, ":", int(b))
	}
	sz2 := Bytes2I(szBytes, 4)

	fmt.Println("len:", len(szBytes))
	fmt.Println(sz2)
}
