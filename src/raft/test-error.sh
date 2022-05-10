#!/bin/bash

for i in {1..100}; do
    echo $i
    go test -run TestFigure8Unreliable2C >2c1.log
    err=($(cat 2c1.log | grep 'error'))
    num=${#err[@]}
    if ! [[ num -eq 0 ]]; then
        exit
    fi
done
