package pbft

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

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

func ReadKeyPair(keyDir string) ([]byte, []byte) {
	Info("read key pair from", keyDir)
	priKey, err := ioutil.ReadFile(keyDir + "/rsa.pri.pem")
	if err != nil {
		Panic("err: %v", err)
	}
	pubKey, err := ioutil.ReadFile(keyDir + "/rsa.pub.pem")
	if err != nil {
		Panic("err: %v", err)
	}
	return priKey, pubKey
}

func RsaSignWithSha256(data []byte, keyBytes []byte) []byte {
	h := sha256.New()
	h.Write(data)
	hashed := h.Sum(nil)
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		panic(errors.New("private key error"))
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		Info("ParsePKCS8PrivateKey err", err)
		panic(err)
	}

	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed)
	if err != nil {
		Info("Error from signing: %s\n", err)
		panic(err)
	}

	return signature
}

//签名验证
func RsaVerifyWithSha256(data, sign, keyBytes []byte) bool {
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		panic(errors.New("public key error"))
	}
	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		panic(err)
	}

	hashed := sha256.Sum256(data)
	err = rsa.VerifyPKCS1v15(pubKey.(*rsa.PublicKey), crypto.SHA256, hashed[:], sign)
	if err != nil {
		panic(err)
	}
	return true
}

func Sha256Digest(msg interface{}) []byte {
	msgBytes := JsonMarshal(msg)

	sha256 := sha256.New()
	sha256.Write(msgBytes)

	return sha256.Sum(nil)
}

func JsonMarshal(msg interface{}) []byte {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		Panic("err: %v", err)
	}
	return msgBytes
}

func VerifySignMsg(signMsg *SignMessage, pubKey []byte) bool {
	//if signMsg.Msg.MsgType == MtPrePrepare {
	//	return VerifyPrePrepareSignMsg(signMsg, pubKey)
	//}
	digest := Sha256Digest(signMsg.Msg)
	result := RsaVerifyWithSha256(digest, signMsg.Sign, pubKey)
	return result
}

func VerifyPrePrepareSignMsg(signMsg *SignMessage, pubKey []byte) bool {
	txs := signMsg.Msg.Req.Txs
	signs := signMsg.Msg.Req.TxSigns
	if txs == nil || signs == nil || len(txs) != len(signs) {
		return false
	}
	digestTime, verifyTime := time.Duration(0), time.Duration(0)
	for idx, tx := range txs {
		sign := signs[idx]
		start := time.Now()
		digest := Sha256Digest(tx)
		digestTime += time.Since(start)
		start = time.Now()
		if !RsaVerifyWithSha256(digest, sign, pubKey) {
			return false
		}
		verifyTime += time.Since(start)
	}
	Info("digestTime:", digestTime)
	Info("verifyTime:", verifyTime)
	return true
}

func SignMsg(msg *Message, priKey []byte) *SignMessage {
	//if msg.MsgType == MtPrePrepare {
	//	return SignPrePrepareMsg(msg, priKey)
	//}
	digest := Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, priKey)
	return &SignMessage{msg, sign}
}

//func SignPrePrepareMsg(msg *Message, priKey []byte) *SignMessage {
//	signMsg := &SignMessage{Msg: msg}
//	if signMsg.Msg.Req.Txs == nil {
//		return signMsg
//	}
//	digestTime, signTime := time.Duration(0), time.Duration(0)
//	signMsg.TxSigns = make([][]byte, 0)
//	for _, tx := range msg.Req.Txs {
//		start := time.Now()
//		digest := Sha256Digest(tx)
//		digestTime += time.Since(start)
//		start = time.Now()
//		sign := RsaSignWithSha256(digest, priKey)
//		signTime += time.Since(start)
//		signMsg.TxSigns = append(signMsg.TxSigns, sign)
//	}
//	Info("digestTime:", digestTime)
//	Info("signTime:", signTime)
//	return signMsg
//}

func SignRequest(msg *Message, priKey []byte) *Message {
	if msg.Txs == nil {
		return msg
	}
	digestTime, signTime := time.Duration(0), time.Duration(0)
	msg.TxSigns = make([][]byte, 0)
	for _, tx := range msg.Txs {
		start := time.Now()
		digest := Sha256Digest(tx)
		digestTime += time.Since(start)
		start = time.Now()
		sign := RsaSignWithSha256(digest, priKey)
		signTime += time.Since(start)
		msg.TxSigns = append(msg.TxSigns, sign)
	}
	Info("digestTime:", digestTime)
	Info("signTime:", signTime)
	return msg
}

func (replica *Replica) signMsg(msg *Message) *SignMessage {
	start := time.Now()
	signMsg := SignMsg(msg, replica.node.priKey)
	signTime := time.Since(start)
	if msg.MsgType == MtPrePrepare {
		replica.stat.signTimeSum += signTime
		replica.stat.signTimeCnt++
	}
	return signMsg
}

func GenRsaKeys(ips []string, ipNum, processNum int) {
	if !IsExist("./certs") {
		Info("检测到还未生成公私钥目录，正在生成公私钥 ...")
		err := os.Mkdir("certs", 0744)
		if err != nil {
			Panic("err: %v", err)
		}
		if err != nil {
			Panic("err: %v", err)
		}
		for _, ip := range ips[:ipNum] {
			for i := 1; i <= processNum; i++ {
				id := GetId(ip, i)
				keyDir := "./certs/" + fmt.Sprint(id)
				if !IsExist(keyDir) {
					err := os.Mkdir(keyDir, 0744)
					if err != nil {
						Panic("err: %v", err)
					}
				}
				pri, pub := GetKeyPair()
				priFilePath := keyDir + "/rsa.pri.pem"
				priFile, err := os.OpenFile(priFilePath, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					Panic("err: %v", err)
				}
				defer priFile.Close()
				priFile.Write(pri)

				pubFilePath := keyDir + "/rsa.pub.pem"
				pubFile, err := os.OpenFile(pubFilePath, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					Panic("err: %v", err)
				}
				defer pubFile.Close()
				pubFile.Write(pub)

				addr := ip + "." + strconv.Itoa(8000+i)
				addrFilePath := keyDir + "/" + addr + ".addr.txt"
				addrFile, err := os.OpenFile(addrFilePath, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					Panic("err: %v", err)
				}
				defer addrFile.Close()
				addrFile.WriteString(ip + ":" + strconv.Itoa(8000+i))
			}
		}
		Info("已为节点们生成RSA公私钥")
	}
}
