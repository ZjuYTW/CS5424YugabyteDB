#ifndef YSQL_STOCK_LEVEL_TXN_H_
#define YSQL_STOCK_LEVEL_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/stock_level_txn.h"

namespace ydb_util {
class YSQLStockLevelTxn : public StockLevelTxn {
 public:
  explicit YSQLStockLevelTxn(pqxx::connection* conn)
      : StockLevelTxn(), conn_(conn) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  pqxx::connection* conn_;
};
}  // namespace ydb_util
#endif