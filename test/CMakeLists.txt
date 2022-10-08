cmake_minimum_required(VERSION 3.2)

set(BINARY ${CMAKE_PROJECT_NAME}_test)

file(GLOB_RECURSE TEST_SRC_FILES   "src/*.cc")

add_executable(${BINARY} ${TEST_SRC_FILES})
target_link_libraries(${BINARY} PUBLIC gtest common_lib ${PQXX_LIB})