package zpbft

import (
	"io/ioutil"
	"math"
	"net"
	"strings"
	"time"
	"zpbft/zlog"
)

func ReadKeyPair(pri, pub string) ([]byte, []byte) {
	prikey, err := ioutil.ReadFile(pri)
	if err != nil {
		zlog.Error("ioutil.ReadFile(pri) filed, err:%v", err)
	}
	pubkey, err := ioutil.ReadFile(pub)
	if err != nil {
		zlog.Error("ioutil.ReadFile(pub) filed, err:%v", err)
	}
	return prikey, pubkey
}

func SliceEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	if (a == nil) != (b == nil) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func I2Bytes(num int64, len int) []byte {
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

func Bytes2I(data []byte, len int) int64 {
	base := int64(1)
	result := int64(0)
	for i := 0; i < len; i++ {
		result += int64(data[i]) * base
		base *= 256
	}
	return result
}

func GetLocalIp() string {

	tcpConn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		zlog.Error("err: %v", err)
	}
	localAddr := tcpConn.LocalAddr().(*net.UDPAddr)
	ip := strings.Split(localAddr.String(), ":")[0]
	return ip
}

func ToSecond(td time.Duration) float64 {
	return float64(td.Nanoseconds()) / math.Pow10(9)
}
