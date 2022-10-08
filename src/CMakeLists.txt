cmake_minimum_required(VERSION 3.2)
project(YCQL_Perf)

add_subdirectory(common)
add_subdirectory(ycql_impl)
set(CQL_SOURCE_FILES ydb_cql_main.cc)
add_executable(ycql_perf ${CQL_SOURCE_FILES})

target_link_libraries(ycql_perf common_lib ${CQL_LIB_PATH} ${SQL_LIB_PATH})

install(TARGETS ycql_perf DESTINATION ${CMAKE_INSTALL_PREFIX})