#查看现有的队列
tc -s qdisc ls dev ${nic}

#查看现有的分类
tc -s class ls dev ${nic}

#清理iptables Mangle规则
iptables -t mangle -F

#清理${nic}上原有的队列类型
tc qdisc del dev ${nic} root

#创建队列
tc qdisc add dev ${nic} root handle 1:0 htb default 1 
#添加一个htb队列，绑定到${nic}上，命名为1:0 ，默认归类为1
#handle：为队列命名或指定某队列

#创建分类
tc class add dev ${nic} parent 1:0 classid 1:1 htb rate 10000Mbit
#为${nic}下的root队列1:0添加一个分类并命名为1:1，类型为htb，带宽为10000M
#rate: 是一个类保证得到的带宽值.如果有不只一个类,请保证所有子类总和是小于或等于父类.

#创建一个子分类
tc class add dev ${nic} parent 1:1 classid 1:10 htb rate 100Mbit ceil 100Mbit
#为1:1类规则添加一个名为1:10的类，类型为htb，带宽为100M
#ceil: ceil是一个类最大能得到的带宽值.
#为了避免一个会话永占带宽,添加随即公平队列sfq. 
tc qdisc add dev ${nic} parent 1:10 handle 10: sfq perturb 10 
#perturb：是多少秒后重新配置一次散列算法，默认为10秒 
#sfq，可以防止一个段内的一个ip占用整个带宽 

#使用u32创建过滤器 
tc filter add dev ${nic} protocol ip parent 1:0 prio 1 u32 match ip sport 46320 flowid 1:10 
#执行此步时总是报错（Illegal “match”） 注意flowid需要与classid相同

#上步执行报错，采用iptables方式限速
#创建过滤器并制定handle
tc filter add dev ${nic} parent 1:0 protocol ip prio 1 handle 10 fw classid 1:10

#使用iptables对端口绑定tc队列
iptables -A OUTPUT -t mangle -p tcp --sport 46320 -j MARK --set-mark 10
#set-mark与classid相同