#!/bin/bash
param=$1

script_dir=$(dirname $0)
cur_dir=$(pwd)
cd ${script_dir}

export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

init()
{
    exit
}

stop()
{
  echo "stop ..."
  ps -elF | grep 'supervise.binstore' | grep -v 'grep' | awk '{print $4}' | xargs -n 1 kill -9
  ps -elF | grep 'bin/binstore' | grep -v 'grep' | awk '{print $4}' | xargs -n 1 kill -9
}

start()
{
  echo "start ..."

  ps -elF | grep 'supervise.binstore' | grep -v 'grep' | awk '{print $4}' | xargs -n 1 kill -9
  ps -elF | grep 'bin/binstore' | grep -v 'grep' | awk '{print $4}' | xargs -n 1 kill -9

  #./bin/supervise.binstore run
  ./bin/supervise.binstore run 
}

restart()
{
  echo "restart ..."
  ps -elF | grep 'bin/binstore' | grep -v 'grep' | awk '{print $4}' | xargs -n 1 kill -9
}

status()
{
	echo 1
	exit
}

case $param in
    'init')
        init
    ;;
    'start')
        start
    ;;
    'stop')
        stop
    ;;
    'restart')
        restart
    ;;
	'status')
		status
	;;
    *)
       start 
    ;;
esac

sleep 1
ps -elF | grep binstore
#pstree | grep binstore

if [ "$param" = "stop" ]; then 
  echo 'done' 
else 
  echo 'done'
fi

cd ${cur_dir}
