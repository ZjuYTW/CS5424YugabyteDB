cmake_minimum_required(VERSION 3.2)

set(SRC_YCQL_IMPL_FILES cql_driver.cc)
add_library(ycql_impl_lib SHARED STATIC ${SRC_YCQL_IMPL_FILES})