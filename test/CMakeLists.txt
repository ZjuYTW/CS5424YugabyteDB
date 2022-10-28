cmake_minimum_required(VERSION 3.2)

set(BINARY ${CMAKE_PROJECT_NAME}_test)

file(GLOB_RECURSE TEST_SRC_FILES   "src/*.cc")

add_executable(${BINARY} ${TEST_SRC_FILES})
target_link_libraries(${BINARY} PUBLIC gtest ysql_impl_lib ycql_impl_lib ${CQL_LIB_PATH} ${SQL_LIB_PATH} ${PQ_LIB_PATH})

