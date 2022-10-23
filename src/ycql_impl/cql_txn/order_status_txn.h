#ifndef YCQL_ORDER_STATUS_TXN_H_
#define YCQL_ORDER_STATUS_TXN_H_
#include "common/txn/order_status_txn.h"

namespace ycql_impl {
class YCQLOrderStatusTxn : public ydb_util::OrderStatusTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLOrderStatusTxn(CassSession* session)
      : OrderStatusTxn(), conn_(session) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
};
}  // namespace ycql_impl

#endif