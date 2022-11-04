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