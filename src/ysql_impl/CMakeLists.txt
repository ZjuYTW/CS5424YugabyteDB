cmake_minimum_required(VERSION 3.2)

file(GLOB_RECURSE SRC_YSQL_IMPL_FILES "*.cc")
add_library(ysql_impl_lib SHARED STATIC ${SRC_YSQL_IMPL_FILES})
if(BUILD_TEST_PERF)
  target_link_libraries(ysql_impl_lib gtest common_lib ${SQL_LIB_PATH})
else()
  target_link_libraries(ysql_impl_lib common_lib ${SQL_LIB_PATH})
endif()