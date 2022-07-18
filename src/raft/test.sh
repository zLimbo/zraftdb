#!/bin/bash

export LOG=1
for i in {1..100}; do
    echo "====> $i"
    echo "" >>test.log
    echo "==========================================================================> $i" >>test.log
    echo "================================> time go test >>test.log 2>&1" >>test.log
    time go test >>test.log 2>&1
    echo "================================> time go test --race >>test.log 2>&1" >>test.log
    time go test --race >>test.log 2>&1
done
