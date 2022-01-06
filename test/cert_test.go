package test

import (
	"testing"
	"zpbft/pbft"
)

func TestCert(t *testing.T) {

	ips := pbft.ReadIps("ips.txt")
	pbft.GenRsaKeys(ips, 1, 4)
}
