# CS5424YugabyteDB

## Usage
#### Prerequisites
You will need
* A modern C/C++ compiler (recommend clang)
  * In case you insist to use g++, check this out [Compile Fail on GCC >= 8](https://github.com/yugabyte/cassandra-cpp-driver/pull/17)
* CMake 3.1+ installed
* libpq, libpqxx (For ysql compile)
* libuv, openssl

#### Building The Project
**Git Clone**
```
mkdir <CS5424_Folder>/workspace
cd <CS5424_Folder>/workspace 
git clone --recurse-submodules https://github.com/ZjuYTW/CS5424YugabyteDB.git

```

**Build**  
Before building the demo, we should first install some dependencies
```
# make sure you have the sudo permission (make init will use apt-get or brew)
# or you have to build by your own
$ make init
```

To make manully  
> For YCQL
```bash
# first we assume you already have the openssl, or you can refer to this https://www.openssl.org/


# second to build libuv on your workspace
pushd /tmp
wget http://dist.libuv.org/dist/v1.14.0/libuv-v1.14.0.tar.gz
tar xzf libuv-v1.14.0.tar.gz
pushd libuv-v1.14.0
sh autogen.sh
./configure -prefix=<your_workspace>
make install
popd
popd

# then you should build the driver manully
$ mkdir tmp && cd tmp
$ git clone https://github.com/yugabyte/cassandra-cpp-driver.git && cd cassandra-cpp-driver
$ mkdir build && cd build
$ cmake .. -DCMAKE_INSTALL_PREFIX=<your_workspace>
$ sudo make
$ sudo make install
# optional: return to YugabyteDB folder and delete tmp dir
$ cd ../../../ && rm -r tmp
```
> For YSQL 
```bash
$ mkdir tmp && cd tmp
$ git clone https://github.com/jtv/libpqxx.git
$ export PATH=$PATH:<yugabyte-install-dir>/postgres/bin
$ cd libpqxx
$ ./configure -prefix=<your_workspace>
$ make
$ make install
# optional: return to YugabyteDB folder and delete tmp dir
$ cd ../../../ && rm -r tmp
```
> Build the Demo
```bash
# make sure you are at YugabyteDB folder
$ make build
```

> Build Options
```bash
# If you want to run gtest (make sure you have read our test file then decide if you want to run)
cmake .. -DBUILD_TEST_PERF=ON
make

# If you want just one cql or sql
cmake .. -DBUILD_CQL_PERF=off -DBUILD_SQL_PERF=on
make
```

> Run Options
```bash
# We use enviornment variable to pass configurations into client.
## For SQL variable, there are 
HOST: ydb host address (default: 127.0.0.1)
PORT: ydb host port (default: 5433)
SERVER_NUM: ydb server number (default 5)
SERVER_IDX: current clients index (default 0)
TXN_NUM: all input file number (default 19, start from 0)

## FOR CQL variable, there are
HOST: ditto
SERVER_NUM: ditto
SERVER_IDX: ditto
TXN_NUM: ditto
SKIP_DELIVERY: skip corresponding txn, for debugging usage. default: false
SKIP_NEW_ORDER: skip corresponding txn, for debugging usage. default: false
SKIP_ORDER_STATUS: skip corresponding txn, for debugging usage. default: false
SKIP_PAYMENT: skip corresponding txn, for debugging usage . default: false
SKIP_POPULAR_ITEM: skip corresponding txn, for debugging usage. default: false
SKIP_RELATED_CUSTOMER: skip corresponding txn, for debugging usage. default: false
SKIP_STOCK_LEVEL: skip corresponding txn, for debugging usage. default: false
SKIP_TOP_BALANCE: skip corresponding txn, for debugging usage. default: false
```

### Script
we have all set automatically booting script on cluster server.
#### Local Side
* Pack the toolbox
```
bash scripts/pack.sh
```

* upload the package to server
```
scp package.zip cs4224k@xcnd25.comp.nus.edu.sg:~
```

#### Server Side
```
unzip package.zip
cd package
```

* Download and write data to yugabyteDB
```
sh prep_data.sh
# write data for cql
./{YDB_LOCAL_PATH}/bin/ycqlsh -f init.cql 192.168.48.244 --request-timeout 60 --connection-timeout 60

# write data for sql
pip install psycopg2 tqdm
python3 write_data.py
```

* Start YDB
```
bash start_ydb.sh
```

* Start testing YSQL
```
bash start_txn.sh ysql
```

* Start testing YCQL
```
bash start_txn.sh ycql
```

* Useful commands
```
# check if server is alive
pgrep yb-master
pgrep yb-server

# check if perf process is alive
pgrep perf
```


### Formatter
We use `clang-format` as the formatter, every time before you commit. Do
```
$ make format
```


