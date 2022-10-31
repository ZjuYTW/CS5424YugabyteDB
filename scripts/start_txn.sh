mkdir -p ./data/output/measure_log/
mkdir -p ./data/output/txn_log/
mkdir -p ./data/output/err_log/
chmod +x ./bin/ysql_perf

Download data
DATA_DIR="./data/xact_files"
if [ -d "$DATA_DIR" ]; then
  echo "Transaction data found, skipping download..."
else
  echo "${DATA_DIR} not found. Downloading data..."
  wget http://www.comp.nus.edu.sg/~cs4224/project_files.zip
  unzip project_files.zip
  mv project_files/xact_files data
  rm -r project_files
fi

# Make sure existing process have been shut down
PACKAGE_DIR="~/package"
ssh cs4224k@xcnd25.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_txns.sh"
ssh cs4224k@xcnd26.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_txns.sh"
ssh cs4224k@xcnd27.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_txns.sh"
ssh cs4224k@xcnd28.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_txns.sh"
ssh cs4224k@xcnd29.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_txns.sh"

rm -r ./client_*.log
rm -r ./data/output/err_log/sql_*
rm -r ./data/output/txn_log/*
rm -r ./data/output/measure_log/*

# pushd ~/workspace/ydb/yugabyte-2.14.1.0
# echo "Restoring Snapshot"
# ./bin/yb-admin -master_addresses 192.168.48.244:7100,192.168.48.245:7100,192.168.48.246:7100 restore_snapshot 2d66d267-1d09-4ce8-9b39-290e206a0d1d
# sleep 10
# echo "Restore Finished"
# popd

ssh cs4224k@xcnd25.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 0 $1 > client_0.log 2>&1 &'"
ssh cs4224k@xcnd26.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 1 $1 > client_1.log 2>&1 &'"
ssh cs4224k@xcnd27.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 2 $1 > client_2.log 2>&1 &'"
ssh cs4224k@xcnd28.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 3 $1 > client_3.log 2>&1 &'"
ssh cs4224k@xcnd29.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 4 $1 > client_4.log 2>&1 &'"