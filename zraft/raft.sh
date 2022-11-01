#!/bin/bash

maddr=$1
echo "./zrf -role raft -maddr 219.228.148.${maddr}:23332 -saddr ${myhost}:23333"
./zrf -role raft -maddr 219.228.148.${maddr}:23332 -saddr ${myhost}:23333
