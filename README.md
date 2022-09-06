# CS5424YugabyteDB

## Usage
#### Prerequisites
You will need
* A modern C/C++ compiler
* CMake 3.1+ installed
* To be added

#### Building The Project (Now a demo)
**Git Clone**
```
mkdir <CS5424_Folder>/workspace
cd <CS5424_Folder>/workspace 
git clone --recurse-submodules\
    https://github.com/ZjuYTW/CS5424YugabyteDB.git

```

**Build**
```
cd CS5424YugabyteDB && mkdir build
cmake ..
make
# To Run the Demo
./build/src/ycql_perf
```

## TODO
* YCQL
  * make sure runnable
  * coding
* YSQL
  * ditto
* Depoly
* Test
* Formatter && linter

