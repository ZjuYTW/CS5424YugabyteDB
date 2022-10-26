#ifndef YSQL_POPULAR_ITEM_TXN_H_
#define YSQL_POPULAR_ITEM_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/popular_item_txn.h"

namespace ydb_util {
class YSQLPopularItemTxn : public PopularItemTxn {
 public:
  explicit YSQLPopularItemTxn(pqxx::connection* conn, std::ofstream& txn_out,
                              std::ofstream& err_out)
      : PopularItemTxn(), conn_(conn), txn_out_(txn_out), err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  std::vector<std::string> outputs;
  static constexpr int MAX_RETRY_COUNT = 3;
  pqxx::connection* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ydb_util
#endif