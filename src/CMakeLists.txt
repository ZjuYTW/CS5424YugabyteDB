cmake_minimum_required(VERSION 3.2)
project(YDB_Perf)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O3")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(common)
add_subdirectory(ycql_impl)
add_subdirectory(ysql_impl)
set(MAIN_SOURCE_FILES main.cc)

add_executable(ydb_perf ${MAIN_SOURCE_FILES})

target_link_libraries(ydb_perf gtest common_lib ${CQL_LIB_PATH} ${SQL_LIB_PATH})

install(TARGETS ydb_perf DESTINATION ${CMAKE_INSTALL_PREFIX})