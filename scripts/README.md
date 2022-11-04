# CS5424YugabyteDB

## Usage
#### Local Side
* Pack the toolbox
```
make build
sh scripts/pack.sh
```

* upload the package to server
```
scp package.zip cs4224k@xcnd25.comp.nus.edu.sg:~
```

#### Server Side
```
unzip package.zip
```

* Start YDB
```
cd package
sh start_ydb.sh
```

* Start testing
```
# start sql transactions
sh start_txn.sh sql

# start cql transactions
sh start_txn.sh cql
```

* Useful commands
```
# check if server is alive
pgrep yb-master
pgrep yb-server

# check if perf process is alive
pgrep perf
```

