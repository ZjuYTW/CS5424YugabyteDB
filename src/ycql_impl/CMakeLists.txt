cmake_minimum_required(VERSION 3.2)

file(GLOB_RECURSE SRC_YCQL_IMPL_FILES "*.cc")
add_library(ycql_impl_lib SHARED STATIC ${SRC_YCQL_IMPL_FILES})
if(BUILD_TEST_PERF)
  target_link_libraries(ycql_impl_lib gtest common_lib)
else()
  target_link_libraries(ycql_impl_lib common_lib)
endif()
