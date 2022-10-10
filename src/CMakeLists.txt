cmake_minimum_required(VERSION 3.2)
project(YDB_Perf)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(common)
add_subdirectory(ycql_impl)
add_subdirectory(ysql_impl)
set(MAIN_SOURCE_FILES main.cc)
set(PQXX /usr/local/include/pqxx)

find_library(CQL_LIB_NAME NAMES cassandra /usr/lib /usr/local/lib)
find_library(PQXX_LIB PQXX)

add_executable(ydb_perf ${MAIN_SOURCE_FILES})

<<<<<<< HEAD
target_link_libraries(ydb_perf common_lib ${CQL_LIB_NAME} ${PQXX_LIB})
=======
target_link_libraries(ycql_perf gtest common_lib ${CQL_LIB_NAME})
>>>>>>> main

install(TARGETS ydb_perf DESTINATION ${CMAKE_INSTALL_PREFIX})