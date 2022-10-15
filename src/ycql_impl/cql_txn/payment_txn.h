#ifndef YCQL_PAYMENT_TXN_H_
#define YCQL_PAYMENT_TXN_H_
#include "common/txn/payment_txn.h"

namespace ydb_util {
class YCQLPaymentTxn : public PaymentTxn {
 public:
  explicit YCQLPaymentTxn(CassSession* session)
      : PaymentTxn(), conn_(session) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
};
}  // namespace ydb_util

#endif