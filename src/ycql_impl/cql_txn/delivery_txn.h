#ifndef YCQL_IMPL_DELIVERY_TXN_H_
#define YCQL_IMPL_DELIVERY_TXN_H_
#include "common/txn/delivery_txn.h"

namespace ydb_util {
class YCQLDeliveryTxn : public DeliveryTxn {
 public:
  explicit YCQLDeliveryTxn(CassSession* session)
      : DeliveryTxn(), conn_(session) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  CassSession* conn_;
};
};  // namespace ydb_util

#endif