cmake_minimum_required(VERSION 3.2)

set(SRC_YSQL_IMPL_FILES sql_driver.cc)
add_library(ysql_impl_lib SHARED STATIC ${SRC_YSQL_IMPL_FILES})