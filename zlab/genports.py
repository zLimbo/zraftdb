#!/bin/python

import sys
import os
import random

hosts = [
    "127.0.0.1"
]


def getPort():
    pscmd = "netstat -ntl | grep -v Active| grep -v Proto | awk '{print $4}' | awk -F: '{print $NF}'"
    procs = os.popen(pscmd).read()
    procarr = procs.split("\n")
    tt = random.randint(15000, 20000)
    if tt not in procarr:
        return tt
    else:
        getPort()


argv = sys.argv
if len(argv) < 2:
    raise Exception("argv.len < 2")
peerNum = int(argv[1])


port = 28000
ports = []
for i in range(0, peerNum):
    # port = getPort()
    port += 1
    ports.append(port)

with open("ports.txt", "w") as f:
    f.write('\n'.join(map(str, ports)))
