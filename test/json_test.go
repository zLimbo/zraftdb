package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"testing"
	"zpbft/pbft"
)

type Config struct {
	PeerIps    []string `json:"peerIps"`
	ClientIp   string   `json:"clientIp"`
	IpNum      int      `json:"ipNum"`
	ProcessNum int      `json:"processNum"`
	ReqNum     int      `json:"reqNum"`
}

func TestJson(t *testing.T) {
	text, err := ioutil.ReadFile("../config/config.json")
	if err != nil {
		pbft.Panic("err: %v", err)
	}
	fmt.Println("text:", string(text))
	var config Config
	err = json.Unmarshal([]byte(text), &config)
	if err != nil {
		pbft.Panic("err: %v", err)
	}
	fmt.Println(config)
	pbft.Info("abc")
	log.Print("def")
}
