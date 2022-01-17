package test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strconv"
	"testing"
	"zpbft/pbft"
)

func IsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		pbft.Warn("err: %v", err)
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

func GenRsaKeys(ips []string, ipNum, processNum int) {
	if !IsExist("./certs") {
		pbft.Info("检测到还未生成公私钥目录，正在生成公私钥 ...")
		err := os.Mkdir("certs", 0744)
		if err != nil {
			pbft.Error("os.Mkdir(\"certs\", 0744), err: %v", err)
		}
		for _, ip := range ips[:ipNum] {
			for i := 1; i <= processNum; i++ {
				id := pbft.GetId(ip, i)
				keyDir := "./certs/" + fmt.Sprint(id)
				if !IsExist(keyDir) {
					err := os.Mkdir(keyDir, 0744)
					if err != nil {
						pbft.Error("os.Mkdir(\"certs\", 0744), err: %v", err)
					}
				}
				pri, pub := GetKeyPair()
				priFilePath := keyDir + "/rsa.pri.pem"
				priFile, err := os.OpenFile(priFilePath, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					pbft.Error("os.OpenFile(priFilePath, os.O_RDWR|os.O_CREATE, 0644), err: %v", err)
				}
				defer priFile.Close()
				priFile.Write(pri)

				pubFilePath := keyDir + "/rsa.pub.pem"
				pubFile, err := os.OpenFile(pubFilePath, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					pbft.Error("os.OpenFile(pubFilePath, os.O_RDWR|os.O_CREATE, 0644), err: %v", err)
				}
				defer pubFile.Close()
				pubFile.Write(pub)

				addr := ip + "." + strconv.Itoa(8000+i)
				addrFilePath := keyDir + "/" + addr + ".addr.txt"
				addrFile, err := os.OpenFile(addrFilePath, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					pbft.Error(" os.OpenFile(addrFilePath, os.O_RDWR|os.O_CREATE, 0644), err: %v", err)
				}
				defer addrFile.Close()
				addrFile.WriteString(ip + ":" + strconv.Itoa(8000+i))
			}
		}
		pbft.Info("已为节点们生成RSA公私钥")
	}
}

func TestGenCert(t *testing.T) {

	ips := pbft.ReadIps("ips.txt")
	fmt.Println("ips:", ips)

	GenRsaKeys(ips, 7, 8)
	fmt.Println("gen certs ok")
}
