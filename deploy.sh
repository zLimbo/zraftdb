#!/bin/bash

client="219.228.148.172"
servers=(
    "219.228.148.45"
    "219.228.148.80"
    "219.228.148.89"
    "219.228.148.129"
    # "219.228.148.169"
    # "219.228.148.181"
    # "219.228.148.222"
    # "219.228.148.231"
)

function deployClient() {
    printf "\n[deployClient]\n"

    printf "deploy zpbft in %-16s ..." ${client}
    start=$(date +%s)
    sshpass -p z scp bin/zpbft z@${client}:~/zpbft/zpbft
    sshpass -p z scp rpbft/rpbft z@${client}:~/zpbft/rpbft
    # sshpass -p z scp -r certs z@${client}:~/zpbft/certs
    echo ${client} >config/local_ip.txt
    sshpass -p z scp -r config/config.json z@${client}:~/zpbft/config/config.json
    end=$(date +%s)
    take=$((end - start))
    printf "\rdeploy zpbft in %-16s ok, take %ds\n" ${client} ${take}
}

function deployServer() {
    printf "\n[deployServer]\n"

    for srv in ${servers[@]}; do
        printf "deploy zpbft in %-16s ..." ${srv}
        start=$(date +%s)
        sshpass -p z scp bin/zpbft z@${srv}:~/zpbft/zpbft
        sshpass -p z scp rpbft/rpbft z@${srv}:~/zpbft/rpbft
        # sshpass -p z scp -r certs z@${srv}:~/zpbft/certs
        echo ${srv} >config/local_ip.txt
        sshpass -p z scp -r config/config.json z@${srv}:~/zpbft/config/config.json
        end=$(date +%s)
        take=$((end - start))
        printf "\rdeploy zpbft in %-16s ok, take %ds\n" ${srv} ${take}
    done
}

if (($# == 0)); then
    echo
    echo echo "please input 'c', 's' or 'a' !"
    exit
fi

go build -o bin/zpbft main/main.go
cd rpbft
go build .
cd ..

if [ $1 == "a" ]; then
    deployClient
    deployServer
elif [ $1 == "c" ]; then
    deployClient
elif [ $1 == "s" ]; then
    deployServer
else
    echo "please input 'c', 's' or 'a' !"
fi
