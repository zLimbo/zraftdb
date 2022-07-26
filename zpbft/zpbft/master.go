package zpbft

import (
	"net/http"
	"net/rpc"
	"sync"
	"zpbft/zlog"
)

type Master struct {
	peerNum int        // 节点数目 = 3f + 1
	addrs   []string   // server注册地址表
	pubkeys [][]byte   // server注册公钥表
	mu      sync.Mutex // 互斥器
	cond    sync.Cond  // 条件变量锁
}

func RunMaster(maddr string, f int) {
	zlog.Info("Master, maddr:%s, f=%d, peerNum=%d", maddr, f, 3*f+1)
	m := &Master{
		peerNum: 3*f + 1,
		addrs:   make([]string, 0),
		pubkeys: make([][]byte, 0),
		cond:    sync.Cond{L: &sync.Mutex{}},
	}
	// 开启 rpc server 监听
	zlog.Info("waiting for node registration ...")
	rpc.Register(m)
	rpc.HandleHTTP()
	err := http.ListenAndServe(maddr, nil) // 此处阻塞
	if err != nil {
		zlog.Error("http.ListenAndServe failed, %v", err)
	}
}

type RegisterArgs struct {
	Addr     string
	Pubkey   []byte
	IsServer bool
}

type RegisterReply struct {
	Addrs   []string
	Pubkeys [][]byte
	Ok      bool
}

// server 注册到 master
func (m *Master) RegisterRpc(args *RegisterArgs, reply *RegisterReply) error {

	// 匿名函数：便于互斥器的加锁与释放
	ok := func() bool {
		m.mu.Lock()
		defer m.mu.Unlock() // 在函数退出时自动执行

		// 如果超过注册数目，则注册失败
		if len(m.addrs) >= m.peerNum {
			return false
		}
		// 如果已经注册，本次注册失败
		for _, addr := range m.addrs {
			if addr == args.Addr {
				return false
			}
		}
		zlog.Info("new peer, addr:%s, id=%d", args.Addr, len(m.addrs))
		// 添加到注册表中
		m.addrs = append(m.addrs, args.Addr)
		m.pubkeys = append(m.pubkeys, args.Pubkey)
		// 达到注册数目，打印消息
		if len(m.addrs) == m.peerNum {
			zlog.Info("All nodes registered successfully, leader.addr=%s", m.addrs[0])
		}
		return true
	}()

	if !ok {
		reply.Ok = false
		return nil
	}

	// 如果注册节点未达到要求，则阻塞
	m.cond.L.Lock() // 条件变量
	for len(m.addrs) < m.peerNum {
		m.cond.Wait()
	}
	m.cond.L.Unlock()
	// 唤醒其他rpc请求让其返回
	m.cond.Broadcast()

	reply.Addrs = m.addrs
	reply.Pubkeys = m.pubkeys
	reply.Ok = true
	return nil
}

type GetPeersArgs struct {
	Addr string
}

type GetPeersReply struct {
	Addrs   []string
	Pubkeys [][]byte
}

// 客户端获取节点信息
func (m *Master) GetPeersRpc(args *GetPeersArgs, reply *GetPeersReply) error {
	zlog.Info("new client addr: %s", args.Addr)
	// 如果注册节点未达到要求，则阻塞
	m.cond.L.Lock()
	for len(m.addrs) < m.peerNum {
		m.cond.Wait()
	}
	m.cond.L.Unlock()
	// 唤醒其他rpc请求让其返回
	m.cond.Broadcast()

	reply.Addrs = m.addrs
	reply.Pubkeys = m.pubkeys
	return nil
}
