#ifndef YSQL_TOP_BALANCE_TXN_H_
#define YSQL_TOP_BALANCE_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/top_balance_txn.h"

namespace ydb_util {
class YSQLTopBalanceTxn : public TopBalanceTxn {
 public:
  explicit YSQLTopBalanceTxn(pqxx::connection* conn, std::ofstream& txn_out,
                             std::ofstream& err_out)
      : TopBalanceTxn(), conn_(conn), txn_out_(txn_out), err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  std::vector<std::string> outputs;
  static constexpr int MAX_RETRY_COUNT = 3;
  static constexpr int TOP_K = 10;

  pqxx::row getWarehouseSQL_(int w_id, pqxx::work* txn);
  pqxx::row getDistrictSQL_(int w_id, int d_id, pqxx::work* txn);
  pqxx::connection* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ydb_util
#endif