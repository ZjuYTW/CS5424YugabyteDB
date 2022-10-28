#include <string>

#include "cassandra.h"
#include "gtest/gtest.h"
#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using namespace std;
TEST(StmtBindTest, Test1) {
  std::string stmt_str = "SELECT * FROM example where key = ?";
  auto statement = cass_statement_new(stmt_str.c_str(), 1);
  auto ret = cql_statement_fill_args(statement, 100);
  ASSERT_EQ(ret, CASS_OK);

  // Too much args, shouldn't ok
  ret = cql_statement_fill_args(statement, 100, 200);
  std::cout << cass_error_desc(ret) << std::endl;
  ASSERT_NE(ret, CASS_OK);
  cass_statement_free(statement);

  stmt_str =
      "UPDATE ybtest.customer SET c_balance = c_balance + ?, c_delivery_cnt "
      "= c_delivery_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
  statement = cass_statement_new(stmt_str.c_str(), 4);
  ret = cql_statement_fill_args(statement, 100, 1, 1, 1);
  ASSERT_EQ(ret, CASS_OK);
}
}  // namespace ycql_impl