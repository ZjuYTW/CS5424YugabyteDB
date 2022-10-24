#ifndef YCQL_ORDER_STATUS_TXN_H_
#define YCQL_ORDER_STATUS_TXN_H_
#include "common/txn/order_status_txn.h"

namespace ydb_util {
class YCQLOrderStatusTxn : public OrderStatusTxn {
 public:
  explicit YCQLOrderStatusTxn(CassSession* session)
      : OrderStatusTxn(), conn_(session) {}

  Status Execute(double* diff_t) noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
};
}  // namespace ydb_util

#endif