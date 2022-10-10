.PHONY: init build clean format lint test

defualt: build

init: install.sh
			sudo bash install.sh

build:
			cmake -B build
			cmake --build build

run_ycql:			
			./build/src/ycql_perf
run_ysql:
			./build/src/ysql_perf

test:
			./build/test/CS5424_YugabyteDB_Perf_test

clean:
			rm -r build
			rm bin/ycql_perf

format:
			bash ./dev-tools/format.sh

