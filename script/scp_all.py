#!/usr/bin/python3
# _*_ coding:utf-8 _*_

import os, sys
import subprocess

with open('config/ips.txt') as f:
    ips = list(f.read().split())

fs = sys.argv[1]

cwd = os.getcwd()
print("cwd", cwd)

subs = []

for ip in ips:
    cmd = 'sshpass -p tongxing scp -r {} tongxing@{}:~/lab'.format(fs, ip)
    print("cmd:", cmd)
    os.system(cmd)
    # sub = subprocess.Popen(cmd)
    # subs.append(sub)
    
# for sub in subs:
#     sub.wait()
print("done.")