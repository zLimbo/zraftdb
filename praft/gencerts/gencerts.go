package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {

	var inPath string
	flag.StringVar(&inPath, "i", "config/ips.cfg", "ips.cfg 文件路径")
	var outPath string
	flag.StringVar(&outPath, "o", "./certs", "certs 输出文件路径")
	var processNum int
	flag.IntVar(&processNum, "n", 8, "每个ip运行进程数")

	ips := ReadIps(inPath)
	Info("ips: \n%v", ips)

	ipNum := len(ips)

	GenRsaKeys(ips, ipNum, processNum, outPath)
}

func GenRsaKeys(ips []string, ipNum int, processNum int, outPath string) {
	if IsExist(outPath) {
		// 存在则删除已有目录
		Info("rm %s", outPath)
		os.RemoveAll(outPath)
	}

	err := os.Mkdir(outPath, 0744)
	if err != nil {
		Error("os.Mkdir(\"certs\", 0744), err: %v", err)
	}
	Info("mkdir %s", outPath)
	Info("gen ...")
	for _, ip := range ips[:ipNum] {
		for i := 1; i <= processNum; i++ {
			id := GetId(ip, i)
			keyDir := "./certs/" + fmt.Sprint(id)
			if !IsExist(keyDir) {
				err := os.Mkdir(keyDir, 0744)
				if err != nil {
					Error("os.Mkdir(\"certs\", 0744), err: %v", err)
				}
			}
			pri, pub := GetKeyPair()
			priFilePath := keyDir + "/rsa.pri.pem"
			priFile, err := os.OpenFile(priFilePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				Error("os.OpenFile(priFilePath, os.O_RDWR|os.O_CREATE, 0644), err: %v", err)
			}
			defer priFile.Close()
			priFile.Write(pri)

			pubFilePath := keyDir + "/rsa.pub.pem"
			pubFile, err := os.OpenFile(pubFilePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				Error("os.OpenFile(pubFilePath, os.O_RDWR|os.O_CREATE, 0644), err: %v", err)
			}
			defer pubFile.Close()
			pubFile.Write(pub)

			addr := ip + "." + strconv.Itoa(8000+i)
			addrFilePath := keyDir + "/" + addr + ".addr.txt"
			addrFile, err := os.OpenFile(addrFilePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				Error(" os.OpenFile(addrFilePath, os.O_RDWR|os.O_CREATE, 0644), err: %v", err)
			}
			defer addrFile.Close()
			addrFile.WriteString(ip + ":" + strconv.Itoa(8000+i))
		}
	}
	Info("gen ok")
}

func IsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		Warn("err: %v", err)
		return false
	}
	return true
}

func GetKeyPair() (prvkey, pubkey []byte) {

	privateKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	derStream := x509.MarshalPKCS1PrivateKey(privateKey)
	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: derStream,
	}
	prvkey = pem.EncodeToMemory(block)
	publicKey := &privateKey.PublicKey
	derPkix, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		panic(err)
	}
	block = &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: derPkix,
	}
	pubkey = pem.EncodeToMemory(block)
	return
}

func GetId(ip string, port int) int64 {
	prefix := int64(0)

	for _, span := range strings.Split(ip, ".")[2:] {
		num, err := strconv.Atoi(span)
		if err != nil {
			Error("err: %v", err)
			return 0
		}
		prefix = prefix*1000 + int64(num)
	}

	id := prefix*int64(100) + int64(port%100)
	return id
}

func ReadIps(path string) []string {

	Info("read %s", path)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		Error("err: %v", err)
	}

	ips := strings.Split(string(data), "\n")
	return ips
}

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

const KLevel = InfoLevel

func init() {
	log.SetFlags(log.Lshortfile)
}

func Debug(format string, v ...interface{}) {
	if KLevel > DebugLevel {
		return
	}
	log.Output(2, fmt.Sprintf("Debug| "+format, v...))
}

func Info(format string, v ...interface{}) {
	if KLevel > InfoLevel {
		return
	}
	log.Output(2, fmt.Sprintf("\033[32m"+"INFO| "+format+"\033[0m", v...))
}

func Warn(format string, v ...interface{}) {
	if KLevel > WarnLevel {
		return
	}
	log.Output(2, fmt.Sprintf("\033[33m"+"WARN| "+format+"\033[0m", v...))
}

func Error(format string, v ...interface{}) {
	s := fmt.Sprintf("\033[31m"+"ERROR| "+format+"\033[0m", v...)
	log.Output(2, s)
	panic(s)
}
