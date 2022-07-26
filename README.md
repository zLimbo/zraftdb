# zpbft

## zraft 实验

> TODO

## zpbft 实验

本实验是一个简单的分布式系统，节点有三种角色：master、server、client。master负责登记server的注册信息（地址和公钥），并将所有server信息发给每个节点，然后server间互相建立P2P连接，等待client的请求到达。Client也需要连接master获取server集群信息，当用户输入命令后，构建请求发给server中的leader，由leader发起共识，在集群中达成一致后，执行该命令，这样所有的server将保持一致的副本状态（每个server拥有独自的kv数据库，但只要按同样顺序执行同样的命令序列，数据库的状态将保持一致）。

实验代码框架如下图所示，zkv是简单的kv数据库，zlog是分级别打印日志，zpbft是共识协议实现，其中有master、server、client的实现代码框架。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps1.jpg) 

主函数代码在naive.go中，其解析命令行参数（下图红框注释部分），通过指定不同的role参数选择不同身份运行。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps2.jpg) 

master.go提供了master功能的全部实现，其具有两个rpc，一个提供给server进行注册，一个提供给client获取server信息，所以master要先启动，这两个rpc会阻塞直至所有server都注册成功后再返回。master需要f参数（即pbft中的容错数）来计算集群需要的server数（3f+1），本实验f默认为1，需要启动四个server去注册，如果f=2，则需要7个。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps3.jpg) 

client.go提供了client功能的全部实现，在连接master获取server信息后，就可以接受用户的命令，构造请求发送给leader，等server进行共识后，server会发送reply给client，client确定接受到f+1个reply消息后就确定该命令执行成功，打印输出结果。下图为client接受命令构造请求通过RPC发送给leader，其他逻辑见代码。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps4.jpg) 

server.go提供了server端的大部分代码，在向master注册信息返回并与其他节点建立连接后，leader等待client请求到达，然后发起一轮pbft共识，经过三阶段的消息交互，执行该命令，改变本节点的zkv数据库状态，并将结果异步返回给client。server的具体逻辑见代码，下图是你需要实现的三个函数，注释给出了相关提示。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps5.jpg) 

## 执行实验代码

实现完上面提及的三个函数后，在项目目录下执行 go build naive.go 进行编译，生成可执行文件。(本实验请在linux环境下进行，其他系统因为换行符格式存在命令解析问题）

先启动master，f参数默认为1可不写。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps6.jpg) 

再启动四个server，下图可以看到id=0，addr=localhost:8001的server成为leader。（默认id=0为leader，但id的分配与谁先注册到master有关)

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps7.jpg) 

最后启动client。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps8.jpg) 

此时master端输出了相关注册信息：

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps9.jpg) 

本实验的zkv数据库支持四种操作。

usage: get <key>, put <key> <val>, del <key>, all

Client输入命令put x 10，如下图所示，右上角为master，右下角为client，左四个框为server，蓝色框内容为pbft共识协议消息交互过程，=》和《=为发送方向，红色框为执行结果，四个server都执行了该命令，对本命令达成共识。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps10.jpg) 

然后通过get x命令可以获取结果，这里我们启动另一个客户端来查看。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps11.jpg) 

此时回去查看server，会发现server又根据这个客户端的命令达成共识。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps12.jpg) 

此时关闭id=3的server（右下角的server），在故障节点为1的情况下仍然能达成共识，见下图，如果故障节点为2，则无法达成共识。

![img](https://raw.githubusercontent.com/zLimbo/image_bed/main/img/wps13.jpg) 

其他的操作可见演示视频。

