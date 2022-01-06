package test

import (
	"fmt"
	"testing"
	"zpbft/pbft"
)

func TestGenCert(t *testing.T) {

	ips := pbft.ReadIps("ips.txt")
	fmt.Println("ips:", ips)

	pbft.GenRsaKeys(ips, 19, 8)
}
