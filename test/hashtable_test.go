package test

import (
	"fmt"
	"testing"
)

func TestHashTable(t *testing.T) {
	table := make(map[int]int)

	for i := 0; i < 20; i++ {
		table[i] = i
	}

	for i := 0; i < 5; i++ {
		fmt.Println(table)
		//for k, v := range table {
		//	fmt.Println("k:", k, ", v:", v)
		//}
	}

}
