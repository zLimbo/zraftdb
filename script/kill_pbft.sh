ps -ef | grep zpbft | grep -v grep | awk '{print $2}' | xargs kill -9
ps -ef | grep zpbft | grep -v grep