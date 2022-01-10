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
	"io/ioutil"
)

func ReadKeyPair(keyDir string) ([]byte, []byte) {
	Trace("read key pair from %s", keyDir)
	priKey, err := ioutil.ReadFile(keyDir + "/rsa.pri.pem")
	if err != nil {
		Error("err: %v", err)
	}
	pubKey, err := ioutil.ReadFile(keyDir + "/rsa.pub.pem")
	if err != nil {
		Error("err: %v", err)
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
		Error("x509.ParsePKCS1PrivateKey(block.Bytes), err: %v", err)
	}

	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed)
	if err != nil {
		Error("Error from signing: %v", err)
	}

	return signature
}

func RsaVerifyWithSha256(data, sign, keyBytes []byte) bool {
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		Error("public key error")
	}
	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		Error("x509.ParsePKIXPublicKey(block.Bytes), err: %v", err)
	}

	hashed := sha256.Sum256(data)
	err = rsa.VerifyPKCS1v15(pubKey.(*rsa.PublicKey), crypto.SHA256, hashed[:], sign)
	if err != nil {
		Error("rsa.VerifyPKCS1v15(...), err: %v", err)
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
		Error("json.Marshal(msg), err: %v", err)
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

func SignMsg(msg *Message, priKey []byte) *SignMessage {
	//if msg.MsgType == MtPrePrepare {
	//	return SignPrePrepareMsg(msg, priKey)
	//}
	digest := Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, priKey)
	return &SignMessage{msg, sign}
}

// func SignMsg(msg *Message, priKey []byte) *Message {
// 	if msg.Txs == nil {
// 		return msg
// 	}
// 	digestTime, signTime := time.Duration(0), time.Duration(0)
// 	msg.TxSigns = make([][]byte, 0)
// 	for _, tx := range msg.Txs {
// 		start := time.Now()
// 		digest := Sha256Digest(tx)
// 		digestTime += time.Since(start)
// 		start = time.Now()
// 		sign := RsaSignWithSha256(digest, priKey)
// 		signTime += time.Since(start)
// 		msg.TxSigns = append(msg.TxSigns, sign)
// 	}
// 	Info("digestTime: %v", digestTime)
// 	Info("signTime: %v", signTime)
// 	return msg
// }

// func VerifyPrePrepareSignMsg(signMsg *SignMessage, pubKey []byte) bool {
// 	txs := signMsg.Msg.Req.Txs
// 	signs := signMsg.Msg.Req.TxSigns
// 	if txs == nil || signs == nil || len(txs) != len(signs) {
// 		return false
// 	}
// 	digestTime, verifyTime := time.Duration(0), time.Duration(0)
// 	for idx, tx := range txs {
// 		sign := signs[idx]
// 		start := time.Now()
// 		digest := Sha256Digest(tx)
// 		digestTime += time.Since(start)
// 		start = time.Now()
// 		if !RsaVerifyWithSha256(digest, sign, pubKey) {
// 			return false
// 		}
// 		verifyTime += time.Since(start)
// 	}
// 	Info("digestTime:", digestTime)
// 	Info("verifyTime:", verifyTime)
// 	return true
// }

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
