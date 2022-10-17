#ifndef YCQL_PAYMENT_TXN_H_
#define YCQL_PAYMENT_TXN_H_
#include "common/txn/payment_txn.h"

namespace ydb_util {
class YCQLPaymentTxn : public PaymentTxn {
 public:
  explicit YCQLPaymentTxn(CassSession* session)
      : PaymentTxn(), conn_(session) {}

  Status Execute() noexcept override;

 private:
  CassSession* conn_;
  static constexpr int MaxRetryCnt = 3;
};
}  // namespace ydb_util

#endif