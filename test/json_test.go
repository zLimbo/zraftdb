package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"testing"
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
		log.Panicln(err)
	}
	fmt.Println("text:", string(text))
	var config Config
	err = json.Unmarshal([]byte(text), &config)
	if err != nil {
		log.Panicln(err)
	}
	fmt.Println(config)
	log.Println("abc")
	log.Print("def")
}
