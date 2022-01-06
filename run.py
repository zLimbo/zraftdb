#!/usr/bin/python3
# _*_ conding:utf-8 _*_

import os
import sys
from multiprocessing import Process

if __name__ == '__main__':

    ipNum, processNum = int(sys.argv[1]), int(sys.argv[2])
    reqNum, reqSize, boostDelay = 0, 0, 3000
    if len(sys.argv) > 3:
        reqNum = int(sys.argv[3])
        if len(sys.argv) > 4:
            reqSize = int(sys.argv[4])
            if len(sys.argv) > 5:
                boostDelay = int(sys.argv[5])

    for processIdx in range(1, processNum + 1):
        log_file = 'log/log_{}_{}.txt'.format(processNum, processIdx)
        cmd = 'go run main.go {} {} {} {} {} {} > {} 2>&1'.format(
            ipNum, processNum, processIdx, reqNum, reqSize, boostDelay, log_file)
        print(">>>", cmd)
        Process(target=os.system, args=(cmd,)).start()
