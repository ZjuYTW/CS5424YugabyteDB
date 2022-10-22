#ifndef YCQL_STOCK_LEVEL_TXN_H_
#define YCQL_STOCK_LEVEL_TXN_H_
#include "common/txn/stock_level_txn.h"

namespace ycql_impl {
class YCQLStockLevelTxn : public ydb_util::StockLevelTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLStockLevelTxn(CassSession* session)
      : StockLevelTxn(), conn_(session) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
};
}  // namespace ycql_impl
#endif