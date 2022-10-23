#ifndef YSQL_STOCK_LEVEL_TXN_H_
#define YSQL_STOCK_LEVEL_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/stock_level_txn.h"

namespace ydb_util {
class YSQLStockLevelTxn : public StockLevelTxn {
 public:
  explicit YSQLStockLevelTxn(pqxx::connection* conn)
      : StockLevelTxn(), conn_(conn) {}

  float Execute() noexcept;

 private:
  int SQL_Get_D_Next_O_ID(int w_id, int d_id, pqxx::work* txn);
  pqxx::result SQL_Get_OL_I_ID(int ol_w_id, int ol_d_id, int d_next_o_id, pqxx::work* txn);
  int SQL_check_stock(int s_w_id, int s_i_id, pqxx::work* txn);

  pqxx::connection* conn_;
  static constexpr int MAX_RETRY_COUNT = 3;
  std::vector<std::string> outputs;
};
}  // namespace ydb_util
#endif