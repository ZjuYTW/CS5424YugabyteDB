#include "gtest/gtest.h"
#include "ycql_impl/defines.h"

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ycql_impl::YCQLKeyspace = "ybtest";
  return RUN_ALL_TESTS();
}