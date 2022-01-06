ps -ef | grep zpbft | awk '{print $2}' | xargs kill -9
ps -ef | grep zpbft