package main

import (
	"flag"
	"os"
	"strconv"
	"zraft/zlog"
	"zraft/zraft"
)

func main() {

	envLevel := 2
	if v := os.Getenv("LOG"); v != "" {
		var err error
		envLevel, err = strconv.Atoi(v)
		if err != nil {
			envLevel = 2
		}
	}

	// 解析命令行参数
	var role, maddr, saddr, caddr string
	var n, logLevel int
	flag.StringVar(&role, "role", "", "role of process")
	flag.StringVar(&maddr, "maddr", "", "address of master")
	flag.StringVar(&saddr, "saddr", "", "address of server")
	flag.StringVar(&caddr, "caddr", "", "address of client")
	flag.IntVar(&n, "n", 3, "peer num")
	flag.IntVar(&logLevel, "log", envLevel, "log level")

	flag.IntVar(&zraft.KConf.EpochSize, "epochSize", 10000, "epoch size")
	flag.IntVar(&zraft.KConf.ReqSize, "reqSize", 128, "req size")
	flag.IntVar(&zraft.KConf.EpochNum, "epochNum", 100, "epoch num")
	flag.IntVar(&zraft.KConf.BatchSize, "batchSize", 1000, "batch size")
	flag.BoolVar(&zraft.KConf.Persisted, "persisted", false, "log out directory")
	flag.StringVar(&zraft.KConf.LogDir, "logDir", "./", "log out directory")
	flag.IntVar(&zraft.KConf.DelayFrom, "delayFrom", 0, "net delay from")
	flag.IntVar(&zraft.KConf.DelayRange, "delayRange", 0, "net delay range")
	flag.BoolVar(&zraft.KConf.Draft, "draft", false, "use draft")

	flag.Parse()

	// 设置日志级别，默认为 Info(1)
	zlog.SetLevel(logLevel)

	// 根据不同身份启动节点
	switch role {
	case "master":
		// 执行命令: ./naive -role master -maddr localhost:8000 [-n 3]
		zraft.RunMaster(maddr, n)
	case "raft":
		// 执行命令: ./naive -role server -maddr localhost:8000 -saddr localhost:800x
		zraft.KConf.Show()
		// 选择使用 raft 还是 draft
		if zraft.KConf.Draft {
			zraft.RunDRaft(maddr, saddr)
		} else {
			zraft.RunRaft(maddr, saddr)
		}

	case "client":
		// 执行命令: ./naive -role client -maddr localhost:8000 -caddr localhost:9000
		// client.RunClient(maddr, caddr)
	default:
		flag.Usage()
	}
}
