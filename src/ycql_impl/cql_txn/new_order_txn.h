#ifndef YCQL_NEW_ORDER_TXN_H_
#define YCQL_NEW_ORDER_TXN_H_
#include "common/txn/new_order_txn.h"

namespace ydb_util {
class YCQLNewOrderTxn : public NewOrderTxn {
 public:
  explicit YCQLNewOrderTxn(CassSession* session)
      : NewOrderTxn(), conn_(session) {}

  float Execute() noexcept override;

 private:
  CassSession* conn_;
};
}  // namespace ydb_util

#endif