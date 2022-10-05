.PHONY: init build clean format lint

defualt: build

init: install.sh
			sudo bash install.sh

build:
			cmake -B build
			cmake --build build

run_ycql:			
			./build/src/ycql_perf

clean:
			rm -r build
			rm bin/ycql_perf

format:
			bash ./dev-tools/format.sh

