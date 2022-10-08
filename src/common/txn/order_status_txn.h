#ifndef YDB_PERF_ORDER_STATUS_TXN_H_
#define YDB_PERF_ORDER_STATUS_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class OrderStatusTxn : public Txn<Connection> {
 public:
  explicit OrderStatusTxn(Connection* conn)
      : Txn<Connection>(TxnType::order_status, conn) {}

  Status ExecuteCQL() noexcept override { return Status::OK(); }
  Status ExecuteSQL() noexcept override { return Status::OK(); }

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    return Status::OK();
  }
};

}  // namespace ydb_util
#endif