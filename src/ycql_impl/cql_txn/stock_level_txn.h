#ifndef YCQL_STOCK_LEVEL_TXN_H_
#define YCQL_STOCK_LEVEL_TXN_H_
#include "common/txn/stock_level_txn.h"

namespace ydb_util {
class YCQLStockLevelTxn : public StockLevelTxn {
 public:
  explicit YCQLStockLevelTxn(CassSession* session)
      : StockLevelTxn(), conn_(session) {}

  float Execute() noexcept override { return 0; }

 private:
  CassSession* conn_;
};
}  // namespace ydb_util
#endif