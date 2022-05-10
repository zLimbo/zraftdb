#!/bin/bash
rm 2a.log
rm 2b.log
rm 2c.log
for i in {1..100}; do
    echo $i
    printf "\n go test -run 2A >>2a.log"
    echo "" >>2a.log
    go test -run 2A >>2a.log
    printf "\n go test -run 2B >>2b.log"
    echo "" >>2b.log
    go test -run 2B >>2b.log
    printf "\n go test -run 2C >>2c.log"
    echo "" >>2c.log
    go test -run 2C >>2c.log
done
