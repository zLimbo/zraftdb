package zpbft

import (
	"bufio"
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"zpbft/zlog"
)

type CmdCert struct {
	seq       int64
	digest    []byte // 请求的摘要
	timestamp int64  // 请求开始时间戳
	replys    map[int32][]byte
	result    string
}

type Client struct {
	mu       sync.Mutex
	maddr    string
	addr     string
	peers    map[int32]*Peer
	f        int
	leader   int32
	seq2cert map[int64]*CmdCert
	applySeq chan int64
}

func RunClient(maddr, caddr string) {

	c := &Client{
		maddr:    maddr,
		addr:     caddr,
		peers:    make(map[int32]*Peer),
		seq2cert: make(map[int64]*CmdCert),
		applySeq: make(chan int64, 100),
	}

	// 开启rpc服务
	c.rpcListen()

	// 从 master 那里获取peers信息
	c.getPeersFromMaster()

	// 连接节点
	c.connectPeers()

	// 开始服务
	c.start()
}

func (c *Client) rpcListen() {
	// 放入协程防止阻塞后面函数
	go func() {
		rpc.Register(c)
		rpc.HandleHTTP()
		if err := http.ListenAndServe(c.addr, nil); err != nil {
			zlog.Error("http.ListenAndServe failed, err:%v", err)
		}
	}()
}

func (c *Client) getPeersFromMaster() {
	time.Sleep(500 * time.Millisecond)
	zlog.Info("connect master ...")
	rpcCli, err := rpc.DialHTTP("tcp", c.maddr)
	if err != nil {
		zlog.Error("rpc.DialHTTP failed, %v", err)
	}

	args := &GetPeersArgs{
		Addr: c.addr,
	}
	reply := &GetPeersReply{}
	rpcCli.Call("Master.GetPeersRpc", args, reply)

	// 设置节点信息
	for i := range reply.Addrs {
		c.peers[int32(i)] = &Peer{
			id:     int32(i),
			addr:   reply.Addrs[i],
			pubkey: reply.Pubkeys[i],
		}
	}

	// n = 3f + 1
	c.f = (len(reply.Addrs) - 1) / 3
	c.leader = 0
}

func (c *Client) connectPeers() {
	zlog.Info("build connect with other peers ...")
	wg := sync.WaitGroup{}
	wg.Add(len(c.peers))
	for _, peer := range c.peers {
		p := peer
		go func() {
			// 每隔1s请求建立连接，10s未连接则报错
			t0 := time.Now()
			for time.Since(t0).Seconds() < 10 {
				rpcCli, err := rpc.DialHTTP("tcp", p.addr)
				if err == nil {
					zlog.Debug("dial (id=%d,addr=%s) success", p.id, p.addr)
					p.rpcCli = rpcCli
					wg.Done()
					return
				}
				zlog.Warn("dial (id=%d,addr=%s) error, err:%v", p.id, p.addr, err)
				time.Sleep(time.Second)
			}
			zlog.Error("connect (id=%d,addr=%s) failed, terminate", p.id, p.addr)
		}()
	}
	wg.Wait()
	zlog.Info("==== connect all peers success ====")
}

func (c *Client) start() {

	time.Sleep(500 * time.Millisecond)
	zlog.Info("client start ...")
	for {
		// 构造请求，发送给 leader，通过共识获得结果
		fmt.Print(">>> ")
		// 读取一行输入
		fmt.Scan()
		cmd, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			zlog.Warn("read input error")
			continue
		}
		// 去掉最后面的换行符
		cmd = cmd[:len(cmd)-1]

		start := time.Now()
		// 构建请求参数
		args := &RequestArgs{
			Req: &RequestMsg{
				ClientAddr: c.addr,
				Timestamp:  time.Now().UnixNano(),
				Command:    cmd,
			},
		}
		reply := &RequestReply{}

		// rpc，失败则重试
		if err := c.peers[c.leader].rpcCli.Call("Server.RequestRpc", args, reply); err != nil {
			zlog.Warn("Call(\"Server.RequestRpc\", args, reply) failed, %v", err)
			continue
		}
		zlog.Info("L=%d seq= %d| 0:request => %d | cmd: [%s]", c.leader, reply.Seq, c.leader, cmd)

		// 匿名函数封装，可以RAII处理锁的获取与释放
		func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			cert := c.getCertOrNew(reply.Seq)
			cert.digest = Digest(args.Req)
			cert.timestamp = start.UnixNano()
		}()

		// 客户端接受f+1个相同回复则将seq放入该通道，从这里获取
		seq := <-c.applySeq
		if seq != reply.Seq {
			zlog.Warn("seq != reply.Seq")
			continue
		}

		// 输出f+1个server返回的相同结果并打印
		take := time.Since(start)
		time.Sleep(100 * time.Millisecond)
		func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			cert := c.getCertOrNew(seq)
			fmt.Println("\n==== result ====")
			fmt.Printf("%s\n", cert.result)
			fmt.Println("================")
			fmt.Printf("take: %v\n\n", take)
		}()
	}
}

// rpc:接受server的回复
func (c *Client) ReplyRpc(args *ReplyArgs, reply *ReplyReply) error {
	reply.Ok = false
	msg := args.Msg

	if msg.ClientAddr != c.addr {
		zlog.Warn("msg.ClientAddr != c.addr")
		return nil
	}

	//  验证签名
	peer := c.peers[args.Msg.PeerId]
	if !Verify(Digest(msg), args.Sign, peer.pubkey) {
		zlog.Warn("ReplyMsg verify error, seq: %d, from: %d", msg.Seq, msg.PeerId)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cert := c.getCertOrNew(msg.Seq)
	// 存入server的签名，已有则无需存入
	if _, ok := cert.replys[msg.PeerId]; ok {
		return nil
	}
	cert.replys[msg.PeerId] = args.Sign

	// 如果暂无result，则存入result
	if cert.result == "" {
		cert.result = args.Msg.Result
	}

	// f+1个消息可确认请求通过了共识,可以输出结果
	replyCount := len(cert.replys)
	zlog.Info("L=%d seq= %d| 4:reply <= %d | reply.count=%d", c.leader, cert.seq, args.Msg.PeerId, replyCount)
	if replyCount == c.f+1 {
		go func() { // 防止阻塞
			c.applySeq <- cert.seq
		}()
	}

	reply.Ok = true
	return nil
}

func (c *Client) getCertOrNew(seq int64) *CmdCert {
	cert, ok := c.seq2cert[seq]
	if !ok {
		cert = &CmdCert{
			seq:    seq,
			replys: make(map[int32][]byte),
		}
		c.seq2cert[seq] = cert
	}
	return cert
}
