# echo "Start test"
# sleep 5
# echo "End test"
mpid=`pgrep yb-master | bc -l`
if [ -z "$mpid" ]
then
    echo "\$yb-master is already stopped"
else
    kill -9 $mpid
fi

spid=`pgrep yb-tserver | bc -l`
if [ -z "$spid" ]
then
    echo "\$yb-server is already stopped"
else
    kill -9 $spid
fi

ppid=`pgrep ysql_perf | bc -l`
if [ -z "$ppid" ]
then
    echo "\$ysql_perf is already stopped"
else
    kill -9 $ppid
fi

ppid=`pgrep ycql_perf | bc -l`
if [ -z "$ppid" ]
then
    echo "\$ycql_perf is already stopped"
else
    kill -9 $ppid
fi