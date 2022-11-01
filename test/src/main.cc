#include "gtest/gtest.h"
#include "ycql_impl/defines.h"

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ycql_impl::YCQLKeyspace = "ybtest";
  ycql_impl::TEST_ERR_OUT_PATH = "test/";
  ycql_impl::TEST_TXN_OUT_PATH = "test/";
  return RUN_ALL_TESTS();
}