package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
	"zpbft/pbft"
)

type Msg struct {
	Ip string `json:"ip"`
}

var HttpTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout: 30 * time.Second,
		KeepAlive: 60 * time.Second,
	}).DialContext,
	MaxIdleConns: 500,
	IdleConnTimeout: 60 * time.Second,
	ExpectContinueTimeout: 30 * time.Second,
	MaxIdleConnsPerHost: 100,
}

var NodeMap map[string]bool
var Ips = pbft.ReadIps("config/ips.txt")

func index(w http.ResponseWriter, req *http.Request) {
	var msg Msg
	err := json.NewDecoder(req.Body).Decode(&msg)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(msg)
	NodeMap[msg.Ip] = true
}

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

func status() {
	for k, v := range NodeMap {
		fmt.Println(k, v)
	}
	time.Sleep(3 * time.Second)
}

func main() {

	
	NodeMap = make(map[string]bool)
	
	http.HandleFunc("/", index)
	go http.ListenAndServe(":8000", nil)

	log.Println("Ips: ", Ips)
	time.Sleep(3 * time.Second)

	go status()

	cli := http.Client{Transport: HttpTransport};

	localIp := pbft.GetLocalIp()
	msg := &Msg{Ip: localIp}
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		log.Panicln(err)
	}
	buf := bytes.NewBuffer(jsonMsg)

	for _, ip := range Ips {
		if ip == localIp {
			continue
		}
		log.Printf("send to %s\n", ip)
		_, err := cli.Post("http://" + ip + ":8000", "application/json", buf)
		if err != nil {
			log.Println(err)
		}
	}

	select {}
}