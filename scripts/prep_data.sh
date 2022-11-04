# Download data
DATA_DIR="./data/xact_files"
if [ -d "$DATA_DIR" ]; then
  echo "Transaction data found, skipping download..."
else
  echo "${DATA_DIR} not found. Downloading data..."
  wget http://www.comp.nus.edu.sg/~cs4224/project_files.zip
  unzip project_files.zip
  mv project_files/xact_files data
  mv project_files/data_files data
  rm -r project_files
fi