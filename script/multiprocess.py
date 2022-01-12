#!/usr/bin/python3
# _*_ coding:utf-8 _*_

import os
import sys
import socket
import json
from multiprocessing import Process


def get_local_ip():
    with open('config/local_ip.txt', 'r') as f:
        ip = f.readline().strip()
        return ip


def load_config():
    with open('./config/config.json', 'r') as f:
        return json.load(f)


def get_local_req_no_list(ipNum, processNum, boostNum, ips):
    localReqNoList = []
    count = 0
    for processIdx in range(processNum):
        for ipIdx in range(ipNum):
            if count >= boostNum:
                return localReqNoList
            if ips[ipIdx] == localIp:
                localReqNoList.append(count)
            count += 1
    return localReqNoList


if __name__ == '__main__':
    config = load_config()
    localIp = get_local_ip()
    print('config:', config)
    print("local ip:", localIp)

    ipNum = config["IpNum"]
    processNum = config["ProcessNum"]
    peerIps = config["PeerIps"][:ipNum]

    if not os.path.exists("./log"):
        os.mkdir("./log")

    if localIp == config["ClientIp"]:
        log_file = 'log/{}-{}.log'.format(processNum, 1)
        cmd = 'bin/zpbft > {}'.format(log_file)
        print(">>>", cmd)
        Process(target=os.popen, args=(cmd,)).start()

    else:
        if not localIp in peerIps:
            print("\n[EXIT] This ip did not participate")
            exit(0)

        # localReqNoList = get_local_req_no_list(
        #     ipNum, processNum, boostNum, ips)
        idx = 0
        for processIdx in range(1, processNum + 1):
            log_file = 'log/{}-{}.log'.format(processNum, processIdx)
            # if idx < len(localReqNoList):
            cmd = 'bin/zpbft -i {} > {} 2>&1'.format(
                processIdx, log_file)
            # idx += 1
            print(">>>", cmd)
            Process(target=os.popen, args=(cmd,)).start()
