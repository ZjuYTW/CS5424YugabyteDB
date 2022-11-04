cmake_minimum_required(VERSION 3.2)

project(CS5424_YugabyteDB_Perf)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O3 -pthread")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_INSTALL_PREFIX "${PROJECT_SOURCE_DIR}/bin/")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(INSTALL_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
set(INSTALL_BIN_DIR ${PROJECT_SOURCE_DIR}/bin)
set(INSTALL_LIB_DIR ${PROJECT_SOURCE_DIR}/lib)

set(HEADERS_DIR ${PROJECT_SOURCE_DIR}/src)

option(BUILD_YCQL_PERF "build ycql perf program" on)
option(BUILD_YSQL_PERF "build ysql perf program" on)
option(BUILD_TEST_PERF "build test program" off)

find_library(CQL_LIB_NAME NAMES cassandra)
find_library(SQL_LIB_NAME NAMES pqxx)
find_library(PQ_LIB_NAME NAMES pq)

if(BUILD_TEST_PERF)
  set(BUILD_YCQL_PERF on)
  set(BUILD_YSQL_PERF on)
endif()

if(BUILD_YCQL_PERF)
  if(CQL_LIB_NAME)
    message("Find libcassandra")
    set(CQL_LIB_PATH ${CQL_LIB_NAME})
  else()
    message(FATAL_ERROR "Can't find libcassandra")
  endif()
endif()

if(BUILD_YSQL_PERF)
  if(SQL_LIB_NAME)
    message("Find libpqxx")
    set(SQL_LIB_PATH ${SQL_LIB_NAME})
  else()
    message(FATAL_ERROR "Can't find libpqxx")
  endif()

  if(PQ_LIB_NAME)
    message("Find libpq")
    set(PQ_LIB_PATH ${PQ_LIB_NAME})
  else()
    message("Can't find libpq")
  endif()
endif()

include_directories(${INSTALL_INCLUDE_DIR})
include_directories(${HEADERS_DIR})
include_directories(third-party/threadpool)

# to introduce YCQL lib
include_directories("/usr/local/include")
include_directories("/opt/homebrew/opt/libpqxx/include" )

if(BUILD_TEST_PERF)
  add_subdirectory(third-party/googletest)
  add_subdirectory(test)
endif()
add_subdirectory(src)