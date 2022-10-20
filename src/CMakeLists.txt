cmake_minimum_required(VERSION 3.2)
project(YDB_Perf)

add_subdirectory(common)
add_subdirectory(ycql_impl)
add_subdirectory(ysql_impl)
set(YSQL_SOURCE_FILES ydb_sql_main.cc)
set(YCQL_SOURCE_FILES ydb_cql_main.cc)

add_executable(ysql_perf ${YSQL_SOURCE_FILES})
add_executable(ycql_perf ${YCQL_SOURCE_FILES})

target_link_libraries(ysql_perf ysql_impl_lib ${CQL_LIB_PATH} ${SQL_LIB_PATH} ${PQ_LIB_PATH})
target_link_libraries(ycql_perf ycql_impl_lib ${CQL_LIB_PATH} ${SQL_LIB_PATH})

install(TARGETS ysql_perf DESTINATION ${CMAKE_INSTALL_PREFIX})
install(TARGETS ycql_perf DESTINATION ${CMAKE_INSTALL_PREFIX})