cmake_minimum_required(VERSION 3.2)
project(YCQL_Perf)

add_subdirectory(parser)
set(SOURCE_FILES main.cc)
find_library(CQL_LIB_NAME NAMES cassandra /usr/lib /usr/local/lib)

add_executable(ycql_perf ${SOURCE_FILES})

target_link_libraries(ycql_perf parser ${CQL_LIB_NAME})

install(TARGETS ycql_perf DESTINATION ${CMAKE_INSTALL_PREFIX})