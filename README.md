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
Before building the demo, we should first install some dependencies
```
$ make init
```
> For YCQL
```bash
# then you should build the driver manully
$ mkdir tmp && cd tmp
$ git clone https://github.com/yugabyte/cassandra-cpp-driver.git && cd cassandra-cpp-driver
$ mkdir build && cd build
$ cmake ..
$ sudo make
$ sudo make install
# optional: return to YugabyteDB folder and delete tmp dir
$ cd ../../../ && rm -r tmp
```
> For YSQL (TBD)
```
```
> Build the Demo
```bash
# make sure you are at YugabyteDB folder
$ make build
```

### Formatter
We use `clang-format` as the formatter, every time before you commit. Do
```
$ make format
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

