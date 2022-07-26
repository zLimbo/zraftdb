#!/bin/bash

hosts=(
    "219.228.148.30"
    "219.228.148.45"
    "219.228.148.80"
    "219.228.148.129"
    "219.228.148.154"
    "219.228.148.178"
    "219.228.148.181"
    "219.228.148.231"
    "219.228.148.237"
)

src='./zrf'
dst="~/z"
spass="sshpass -p z"

printf "\n[deploy]\n"

for host in ${hosts[@]}; do
    printf "deploy in %-16s ..." ${host}
    start=$(date +%s)

    if ! ssh z@${host} test -e ${dst}; then
        echo "mkdir ${dst}"
        ssh z@${host} mkdir -p ${dst}
    fi
    $spass scp -r ${src} z@${host}:${dst}

    end=$(date +%s)
    take=$((end - start))
    printf "\rdeploy in %-16s ok, take %ds\n" ${host} ${take}
done
