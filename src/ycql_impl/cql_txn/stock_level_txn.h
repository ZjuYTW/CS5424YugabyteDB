#ifndef YCQL_STOCK_LEVEL_TXN_H_
#define YCQL_STOCK_LEVEL_TXN_H_
#include "common/txn/stock_level_txn.h"

namespace ydb_util {
class YCQLStockLevelTxn : public ydb_util::StockLevelTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLStockLevelTxn(CassSession* session)
      : StockLevelTxn(), conn_(session) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
};
}  // namespace ydb_util
#endif