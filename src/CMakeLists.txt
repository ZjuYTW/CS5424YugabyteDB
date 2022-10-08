cmake_minimum_required(VERSION 3.2)
project(YCQL_Perf)

add_subdirectory(common)
add_subdirectory(ycql_impl)
set(CQL_SOURCE_FILES ydb_cql_main.cc)
find_library(CQL_LIB_NAME NAMES cassandra)
find_library(SQL_LIB_NAME NAMES pqxx)

if(CQL_LIB_NAME)
  message("Find libcassandra")
  set(CQL_LIB_PATH ${CQL_LIB_NAME})
else()
  message(FATAL_ERROR "Can't find libcassandra")
endif()

if(SQL_LIB_NAME)
  message("Find libpqxx")
  set(SQL_LIB_PATH ${SQL_LIB_NAME})
else()
  message(FATAL_ERROR "Can't find libpqxx")
endif()

add_executable(ycql_perf ${CQL_SOURCE_FILES})

target_link_libraries(ycql_perf common_lib ${CQL_LIB_PATH} ${SQL_LIB_PATH})

install(TARGETS ycql_perf DESTINATION ${CMAKE_INSTALL_PREFIX})