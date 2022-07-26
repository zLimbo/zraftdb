package zpbft

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"zpbft/zlog"
)

func Sign(msg []byte, prikey []byte) []byte {
	h := sha256.New()
	h.Write(msg)
	hashed := h.Sum(nil)
	block, _ := pem.Decode(prikey)
	if block == nil {
		zlog.Error("private key error")
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		zlog.Error("x509.ParsePKCS1PrivateKey(block.Bytes), err: %v", err)
	}
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed)
	if err != nil {
		zlog.Error("Error from signing: %v", err)
	}
	return signature
}

func Verify(msg, signature, pubkey []byte) bool {
	block, _ := pem.Decode(pubkey)
	if block == nil {
		zlog.Warn("public key error")
		return false
	}
	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		zlog.Warn("x509.ParsePKIXPublicKey(block.Bytes), err: %v", err)
		return false
	}
	hashed := sha256.Sum256(msg)
	if err = rsa.VerifyPKCS1v15(pubKey.(*rsa.PublicKey), crypto.SHA256, hashed[:], signature); err != nil {
		zlog.Warn("rsa.VerifyPKCS1v15(...), err: %v", err)
		return false
	}
	return true
}

func Digest(msg interface{}) []byte {
	msgBytes := JsonMarshal(msg)
	sha256 := sha256.New()
	sha256.Write(msgBytes)
	return sha256.Sum(nil)
}

func JsonMarshal(msg interface{}) []byte {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		zlog.Error("json.Marshal(msg), err: %v", err)
	}
	return msgBytes
}
