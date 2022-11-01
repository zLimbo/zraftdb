package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
)

func ReadKeyPair(keyDir string) ([]byte, []byte) {
	Debug("read key pair from %s", keyDir)
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
		Error("private key error")
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
		Warn("public key error")
		return false
	}
	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		Warn("x509.ParsePKIXPublicKey(block.Bytes), err: %v", err)
		return false
	}

	hashed := sha256.Sum256(data)
	err = rsa.VerifyPKCS1v15(pubKey.(*rsa.PublicKey), crypto.SHA256, hashed[:], sign)
	if err != nil {
		Warn("rsa.VerifyPKCS1v15(...), err: %v", err)
		return false
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
	return make([]byte,100)
	//msgBytes, err := json.Marshal(msg)
	//if err != nil {
	//	Error("json.Marshal(msg), err: %v", err)
	//}
	//return msgBytes
}
