package pbft

import (
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"strings"
)

const kLocalIpFile = "./config/local_ip.txt"

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
	ipBytes, err := ioutil.ReadFile(kLocalIpFile)
	if err == nil {
		ip := string(ipBytes)
		log.Println("** get ip from local_ip.txt, ip:", ip)
		return strings.TrimSpace(ip)
	}

	tcpConn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		log.Panic(err)
	}
	localAddr := tcpConn.LocalAddr().(*net.UDPAddr)
	log.Println("localAddr:", localAddr.String())
	ip := strings.Split(localAddr.String(), ":")[0]
	log.Println("** get ip from dial, ip:", ip)

	if err = ioutil.WriteFile(kLocalIpFile, []byte(ip), 0644); err != nil {
		log.Panic(err)
	}
	return ip
}

func ReadIps(path string) []string {

	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Panic(err)
	}

	ips := strings.Split(string(data), "\n")
	// if len(ips) == 1 {
	// 	log.Panic("read Ips error!")
	// }
	return ips
}

func Addr2Id(addr string) int64 {
	list := strings.Split(addr, ":")
	ip, portStr := list[0], list[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Panic(err)
	}
	return GetId(ip, port)
}

func GetId(ip string, port int) int64 {
	prefix := int64(0)

	for _, span := range strings.Split(ip, ".")[2:] {
		num, err := strconv.Atoi(span)
		if err != nil {
			log.Panic(err)
			return 0
		}
		prefix = prefix*1000 + int64(num)
	}

	id := prefix*int64(100) + int64(port%100)
	return id
}
