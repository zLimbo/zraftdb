#!/bin/bash

peerNum=$1
echo "./zrf -role master -maddr ${myhost}:23332 -n $peerNum"
./zrf -role master -maddr ${myhost}:23332 -n $peerNum
