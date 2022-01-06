package test

import (
	"fmt"
	"io/ioutil"
	"testing"
	"zpbft/pbft"
)

func TestLocalIP(t *testing.T) {
	ip := pbft.GetLocalIp()
	fmt.Println("local ip:", ip)

	// ip = "127.0.0.1"
	err := ioutil.WriteFile("local_ip.txt", []byte(ip), 0644)
	if err != nil {
		fmt.Println(err)
	}
}
