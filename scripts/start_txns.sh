mkdir -p ./data/output/measure_log/
mkdir -p ./data/output/txn_log/
mkdir -p ./data/output/err_log/
# chmod +x ./setup.sh

# Download data
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

ssh cs4224k@xcnd25.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 0 > client_0.log 2>&1 &'"
ssh cs4224k@xcnd26.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 1 > client_1.log 2>&1 &'"
ssh cs4224k@xcnd27.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 2 > client_2.log 2>&1 &'"
ssh cs4224k@xcnd28.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 3 > client_3.log 2>&1 &'"
ssh cs4224k@xcnd29.comp.nus.edu.sg "sh -c 'cd package && nohup ./setup.sh 4 > client_4.log 2>&1 &'"