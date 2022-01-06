#!/usr/bin/python3
import os
import sys


if __name__ == '__main__':
    processNum = int(sys.argv[1])
    op = sys.argv[2]
    if op == 'run':
        for processIdx in range(1, processNum + 1):
            docker_cmd = "docker run -itd --name pbft{} -p 1000{}:1000{} -v /home/tongxing/z:/z -v /usr:/usr" \
                         " centos:centos7_1 bash".format(processIdx, processIdx, processIdx)
            print(">>>", docker_cmd)
            os.system(docker_cmd)
            
    else:
        for processIdx in range(1, processNum + 1):
            docker_cmd = "docker {} pbft{}".format(op, processIdx)
            print(">>>", docker_cmd)
            os.system(docker_cmd)
