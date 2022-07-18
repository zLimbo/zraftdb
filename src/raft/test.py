#!/bin/python3

import os

err_msg = "config.go:551: one(99) failed to reach agreement"

for i in range(100):
    print("go test -run RPCBytes2B --race > 2b.log ====>", i)
    os.system("go test -run RPCBytes2B --race > 2b.log")
    with open("2b.log") as f:
        if err_msg in f.read():
            print("error log!")
            exit(0)
