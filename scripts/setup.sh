echo "Setting up environment variables..."
export YDB_HOST=192.168.48.244
export YDB_SQL_PORT=5433
export YDB_CQL_PORT=9042
export YDB_SERVER_NUM=5
export YDB_SERVER_IDX=$1
export YDB_TXN_NUM=19
 
# Run Transactions
echo "Starting program..."
./bin/ysql_perf
echo "setup Finished"


