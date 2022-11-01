#!/bin/bash

client=""
servers=(
)

function deployClient() {
    printf "\n[deployClient]\n"

    printf "deploy client in %-16s ..." ${client}
    start=$(date +%s)

    echo ${client} >config/local_ip.txt
    sshpass -p z scp -r config z@${client}:~/zpbft/config
    sshpass -p z scp bin/zpbft z@${client}:~/zpbft/zpbft
    # sshpass -p z scp -r certs z@${client}:~/zpbft/certs

    sshpass -p z scp -r config/config.json z@${client}:~/zpbft/config/config.json

    end=$(date +%s)
    take=$((end - start))
    printf "\rdeploy client in %-16s ok, take %ds\n" ${client} ${take}
}

function deployServer() {
    printf "\n[deployServer]\n"

    for srv in ${servers[@]}; do
        printf "deploy server in %-16s ..." ${srv}
        start=$(date +%s)

        echo ${srv} >config/local_ip.txt
        # sshpass -p z scp -r config z@${srv}:~/zpbft/config
        sshpass -p z scp bin/zpbft z@${srv}:~/zpbft/zpbft
        # sshpass -p z scp -r certs z@${srv}:~/zpbft/certs

        sshpass -p z scp -r config/config.json z@${srv}:~/zpbft/config/config.json

        end=$(date +%s)
        take=$((end - start))
        printf "\rdeploy server in %-16s ok, take %ds\n" ${srv} ${take}
    done
}

if (($# == 0)); then
    echo
    echo echo "please input 'c', 's' or 'a' !"
    exit
fi

printf "\n[compile]\n"
printf "compile zpbft ..."
start=$(date +%s)

./build.sh

end=$(date +%s)
take=$((end - start))
printf "\rcompile zpbft, rpbft ok, take %ds\n" ${take}

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
