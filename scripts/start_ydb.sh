PACKAGE_DIR="~/package"
WORKSPACE_DIR="~/workspace/ydb/yugabyte-2.14.1.0"
LOG_DIR="/mnt/ramdisk/group_k"

# Make sure existing process have been shut down
ssh cs4224k@xcnd25.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_ydb.sh"
ssh cs4224k@xcnd26.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_ydb.sh"
ssh cs4224k@xcnd27.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_ydb.sh"
ssh cs4224k@xcnd28.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_ydb.sh"
ssh cs4224k@xcnd29.comp.nus.edu.sg \
    "cd ${PACKAGE_DIR} && ./kill_ydb.sh"

echo "Kill existing service finished..."

sleep 2

echo "Starting master nodes..."

# Start Master
ssh cs4224k@xcnd25.comp.nus.edu.sg \
    "sh -c 'cd ${WORKSPACE_DIR} && ./bin/yb-master --flagfile ../../cfg_ram/master25.conf > ${LOG_DIR}/yb-master.out 2>&1 &'"
ssh cs4224k@xcnd26.comp.nus.edu.sg \
    "sh -c 'cd ${WORKSPACE_DIR} && ./bin/yb-master --flagfile ../../cfg_ram/master26.conf > ${LOG_DIR}/yb-master.out 2>&1 &'"
ssh cs4224k@xcnd27.comp.nus.edu.sg \
    "sh -c 'cd ${WORKSPACE_DIR} && ./bin/yb-master --flagfile ../../cfg_ram/master27.conf > ${LOG_DIR}/yb-master.out 2>&1 &'"

echo "Master nodes started."
# Pausing for master to elect
sleep 5

echo "Starting server nodes..."
# Starting Server
ssh cs4224k@xcnd25.comp.nus.edu.sg \
    "sh -c 'cd ${WORKSPACE_DIR} && ./bin/yb-tserver --flagfile ../../cfg_ram/server25.conf > ${LOG_DIR}/yb-tserver.out 2>&1 &'"
ssh cs4224k@xcnd26.comp.nus.edu.sg \
    "sh -c 'cd ${WORKSPACE_DIR} && ./bin/yb-tserver --flagfile ../../cfg_ram/server26.conf > ${LOG_DIR}/yb-tserver.out 2>&1 &'"
ssh cs4224k@xcnd27.comp.nus.edu.sg \
    "sh -c 'cd ${WORKSPACE_DIR} && ./bin/yb-tserver --flagfile ../../cfg_ram/server27.conf > ${LOG_DIR}/yb-tserver.out 2>&1 &'"
ssh cs4224k@xcnd28.comp.nus.edu.sg \
    "sh -c 'cd ${WORKSPACE_DIR} && ./bin/yb-tserver --flagfile ../../cfg_ram/server28.conf > ${LOG_DIR}/yb-tserver.out 2>&1 &'"
ssh cs4224k@xcnd29.comp.nus.edu.sg \
    "sh -c 'cd ${WORKSPACE_DIR} && ./bin/yb-tserver --flagfile ../../cfg_ram/server29.conf > ${LOG_DIR}/yb-tserver.out 2>&1 &'"
    
echo "Server nodes started."