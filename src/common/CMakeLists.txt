cmake_minimum_required(VERSION 3.2)

file(GLOB_RECURSE SRC_COMMON_FILES "parser/*.cc"
                                   "txn/*.cc"
                                   "util/*.cc"
)

add_library(common_lib SHARED STATIC ${SRC_COMMON_FILES})
if(BUILD_TEST_PERF)
  target_compile_definitions(common_lib PUBLIC BUILD_TEST_PERF="1")
endif()