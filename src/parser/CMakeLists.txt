cmake_minimum_required(VERSION 3.2)
project(parser C CXX)

set(SOURCE_FILES parser.h parser.cc)

add_library(parser SHARED STATIC ${SOURCE_FILES})

install(TARGETS parser DESTINATION ${INSTALL_LIB_DIR})
install(FILES parser.h DESTINATION ${INSTALL_INCLUDE_DIR})