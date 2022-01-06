
#!/usr/bin/python3
# _*_ coding:utf-8 _*_

import os, sys
import subprocess

with open('config/ips.txt') as f:
    ips = list(f.read().split())

cmd = ' '.join(sys.argv[1:])
print("cmd:", cmd)

send_cmd = 'sshpass -p tongxing ' + cmd
os.system(send_cmd)


# for ip in ips:
#     cmd = 'sshpass -p tongxing scp -r {} tongxing@{}:~/lab'.format(fs, ip)
#     print("cmd:", cmd)
#     os.system(cmd)
    # sub = subprocess.Popen(cmd)
    # subs.append(sub)
    
# for sub in subs:
#     sub.wait()
print("done.")