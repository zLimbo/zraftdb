#!/usr/bin/python3
# _*_ coding:utf-8 _*_

import os
import sys
import socket
from multiprocessing import Process


def get_host_ip():
    try:
        with open('../../local_ip.txt', 'r') as f:
            ip = f.readline().strip()
            print("# get ip from local_ip.txt, ip:", ip)
            return ip
    except:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    with open('../../local_ip.txt', 'w') as f:
        f.write(ip)
    print("# get ip from dial, ip:", ip)
    return ip


def get_nodes():
    ips = []
    with open('ips.txt', 'r') as f:
        for line in f:
            ips.append(line.strip())
    return ips


def get_local_req_no_list(ipNum, processNum, boostNum):
    localReqNoList = []
    count = 0
    for processIdx in range(processNum):
        for ipIdx in range(ipNum):
            if count >= boostNum:
                return localReqNoList
            if ips[ipIdx] == hostIp:
                localReqNoList.append(count)
            count += 1
    return localReqNoList


if __name__ == '__main__':
    ips = get_nodes()
    print("ips:", ips)

    # input: ipNum processNum boostNum reqNum [boostDelayGap [boostDelayNum]]
    ipNum, processNum, boostNum, reqNum = map(int, sys.argv[1:5])
    boostDelayGap, boostDelayNum = 0, 1
    if len(sys.argv) > 5:
        boostDelayGap = int(sys.argv[5])
        if len(sys.argv) > 6:
            boostDelayNum = int(sys.argv[6])

    hostIp = get_host_ip()
    localReqNoList = get_local_req_no_list(ipNum, processNum, boostNum)
    print("# ipNum={}".format(ipNum))
    print("# processNum={}".format(processNum))
    print("# boostNum={}".format(boostNum))
    print("# hostIp={}".format(hostIp))
    print("# localReqNoList(len={})={}".format(len(localReqNoList), localReqNoList))
    print("# boostDelayGap={}".format(boostDelayGap))
    print("# boostDelayNum={}".format(boostDelayNum))

    idx = 0
    ps = []
    for processIdx in range(1, processNum + 1):
        log_file = 'log/log_{}_{}.txt'.format(processNum, processIdx)
        if idx < len(localReqNoList):
            boostDelay = 3000 + boostDelayGap * (localReqNoList[idx] // boostDelayNum)
            pbft_cmd = './zpbft {} {} {} {} {} {} > {} 2>&1'.format(
                ipNum, processNum, processIdx, boostNum, reqNum, boostDelay, log_file)
            idx += 1
        else:
            pbft_cmd = './zpbft {} {} {} {} > {} 2>&1'.format(ipNum, processNum, processIdx, boostNum, log_file)
        docker_cmd = "docker exec -it pbft{} bash -c 'cd /z/zpbft/zpbft && {}'".format(processIdx, pbft_cmd)
        print(">>>", docker_cmd)
        p = Process(target=os.popen, args=(docker_cmd,))
        ps.append(p)
        p.start()

