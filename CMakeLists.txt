cmake_minimum_required(VERSION 3.2)

project(CS5424_YugabyteDB_Perf)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O3")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_INSTALL_PREFIX "${PROJECT_SOURCE_DIR}/bin/")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(INSTALL_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
set(INSTALL_BIN_DIR ${PROJECT_SOURCE_DIR}/bin)
set(INSTALL_LIB_DIR ${PROJECT_SOURCE_DIR}/lib)

set(HEADERS_DIR ${PROJECT_SOURCE_DIR}/src)

include_directories(${INSTALL_INCLUDE_DIR})
include_directories(${HEADERS_DIR})
include_directories(third-party/threadpool)

# to introduce YCQL lib
include_directories("/usr/local/include")

add_subdirectory(third-party/googletest)
add_subdirectory(src)
add_subdirectory(test)
