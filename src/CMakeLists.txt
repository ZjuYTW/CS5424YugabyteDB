cmake_minimum_required(VERSION 3.2)
project(YCQL_Perf)

add_subdirectory(parser)
set(SOURCE_FILES main.cc)

add_executable(ycql_perf ${SOURCE_FILES})
target_link_libraries(ycql_perf parser)
install(TARGETS ycql_perf DESTINATION ${INSTALL_BIN_DIR})