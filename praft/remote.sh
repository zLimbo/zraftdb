#!/bin/bash

client="219.228.148.154"
servers=(
    "219.228.148.45"
    "219.228.148.80"
    "219.228.148.129"
    "219.228.148.154"
    "219.228.148.178"
    "219.228.148.181"
    "219.228.148.222"
)

dst="/home/z/zpbft"

if (($# == 0)); then
    printf "   ./remote.sh k\n"
    printf "or ./remote.sh r\n"
    exit
fi

time=$(date "+%Y%m%d-%H%M%S")
cmd="cd ${dst}; ./zpbft > ${time}.log 2>&1 &"

printf "\n[kill zpbft]\n"
for srv in ${servers[@]}; do
    printf "server %s kill\n" ${srv}
    sshpass -p z ssh z@${srv} "cd ${dst}; ./kill.sh"
done
sshpass -p z ssh z@${client} "cd ${dst}; ./kill.sh"

if [ ! $1 == "r" ]; then
    exit
fi

sleep 1

printf "\n[run server]\n"

for srv in ${servers[@]}; do
    printf "run %s..." ${srv}
    sshpass -p z ssh z@${srv} ${cmd}
    printf "\rrun %s ok\n" ${srv}
done

sleep 1

printf "\n[run client]\n"
printf "run %s..." ${client}
sshpass -p z ssh z@${client} ${cmd}
printf "\rrun %s ok\n" ${client}

echo "scp z@${client}:~/2zp/${time}.log ."
