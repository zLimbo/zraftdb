#!/usr/bin/python3
# _*_ coding:utf-8 _*_

import os
import sys

content = """
#!/bin/bash
# 针对不同的ip进行限速

# 清空原有规则
tc qdisc del dev docker0 root

# 创建根序列
tc qdisc add dev docker0 root handle 1: htb default 1

# 创建一个主分类绑定所有带宽资源（100Mbit）
tc class add dev docker0 parent 1:0 classid 1:1 htb rate 100Mbit burst 15k

# 创建子分类
tc class add dev docker0 parent 1:1 classid 1:20 htb rate 100Mbit ceil 100Mbit burst 15k

# 避免一个ip霸占带宽资源
tc qdisc add dev docker0 parent 1:20 handle 20: sfq perturb 10

# 创建过滤器, 内网ip限制为100Mbit/s
tc filter add dev docker0 protocol ip parent 1:0 prio 1 u32 match ip dst 10.11.0.0/16 flowid 1:20
"""

un_tc = """
#!/bin/bash
# 清空原有规则
tc qdisc del dev docker0 root
"""

if __name__ == '__main__':
    if len(sys.argv) > 1:
        bandwidth = int(sys.argv[1])
        content = content.replace("100Mbit", str(bandwidth) + "Mbit")
    with open("tc_docker0.sh", "w", encoding="utf-8") as f:
        f.write(content)
    with open("config/nic_name", "r", encoding="utf-8") as f:
        nic_name = f.read().strip()
    content = content.replace("docker0", nic_name)
    with open("script/tc.sh", "w", encoding="utf-8") as f:
        f.write(content)
    un_tc = un_tc.replace("docker0", nic_name)
    with open("script/un_tc.sh", "w", encoding="utf-8") as f:
        f.write(un_tc)
