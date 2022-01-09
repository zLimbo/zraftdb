package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"
	"zpbft/pbft"
)

type Msg struct {
	Ip string `json:"ip"`
}

var HttpTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 60 * time.Second,
	}).DialContext,
	MaxIdleConns:          500,
	IdleConnTimeout:       60 * time.Second,
	ExpectContinueTimeout: 30 * time.Second,
	MaxIdleConnsPerHost:   100,
}

var RecvMap map[string]bool
var SendMap map[string]bool
var Ips = pbft.ReadIps("config/ips.txt")
var LocalIp = pbft.GetLocalIp()

const Port = 10008

func index(w http.ResponseWriter, req *http.Request) {
	var msg Msg
	err := json.NewDecoder(req.Body).Decode(&msg)
	if err != nil {
		// Panic("err: %v", err)
		return
	}
	if RecvMap[msg.Ip] {
		return
	}
	// Info("recv:", msg)
	RecvMap[msg.Ip] = true
}

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

func status() {
	for {
		cmd := exec.Command("clear")
		cmd.Stdout = os.Stdout
		err := cmd.Run()
		if err != nil {
			continue
		}
		// time.Sleep(20 * time.Second)
		for idx, ip := range Ips {
			if LocalIp == ip {
				continue
			}
			format := "[" + strconv.Itoa(idx) + "]" + LocalIp + " \033[%dm<==\033[0m \033[%dm==>\033[0m " + ip + "\n"
			var recv, send int
			if RecvMap[ip] {
				recv = 32
			} else {
				recv = 31
			}
			if SendMap[ip] {
				send = 32
			} else {
				send = 31
			}
			fmt.Printf(format, recv, send)
		}
		// RecvMap = make(map[string]bool)
		// SendMap = make(map[string]bool)
		time.Sleep(3 * time.Second)
	}
}

func dial() {
	cli := http.Client{Transport: HttpTransport}

	msg := &Msg{Ip: LocalIp}
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		pbft.Error("err: %v", err)
	}
	for {
		for _, ip := range Ips {
			if ip == LocalIp {
				continue
			}
			// Info("send to %s\n", ip)
			buf := bytes.NewBuffer(jsonMsg)
			_, err := cli.Post("http://"+ip+":"+strconv.Itoa(Port), "application/json", buf)
			if err != nil {
				// fmt.Println("send fail:", ip)
				SendMap[ip] = false
			} else {
				SendMap[ip] = true
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func main() {

	RecvMap = make(map[string]bool)
	SendMap = make(map[string]bool)

	go func() {
		http.HandleFunc("/", index)
		err := http.ListenAndServe("0.0.0.0:"+strconv.Itoa(Port), nil)
		if err != nil {
			pbft.Error("err: %v", err)
		}
	}()

	// Info("Ips: ", Ips)
	time.Sleep(3 * time.Second)

	go dial()
	go status()

	select {}
}
