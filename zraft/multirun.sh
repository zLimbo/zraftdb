#!/bin/bash

maddr=$1
peerNum=$2

python3 genports.py $peerNum

ports=($(cat ports.txt))

for port in ${ports[@]}; do
    echo "./zrf -role raft -maddr 219.228.148.${maddr}:23332 -saddr ${myhost}:${port}"
    ./zrf -role raft -maddr 219.228.148.${maddr}:23332 -saddr ${myhost}:${port} -log 0 >log/${port}.log 2>&1 &
done
