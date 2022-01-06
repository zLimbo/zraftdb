#!/bin/bash

text="$(cat config/ips.txt)"
# echo $text
# OLD_IFS="$IFS"
# IFS="\n"
# ips=($text)
# IFS="$OLD_IFS"

for i in "${!text[@]}"; do
    echo $i
done
