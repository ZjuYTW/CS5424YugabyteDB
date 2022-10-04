#ifndef YDB_PERF_STOCK_LEVEL_TXN_H_
#define YDB_PERF_STOCK_LEVEL_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class StockLevelTxn : public Txn<Connection> {
 public:
  explicit StockLevelTxn(Connection* conn)
      : Txn<Connection>(TxnType::stock_level, conn) {}

  Status ExecuteCQL() noexcept override;
  Status ExecuteSQL() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

}  // namespace ydb_util
#endif