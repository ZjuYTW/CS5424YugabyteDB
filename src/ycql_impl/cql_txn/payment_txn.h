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

  std::pair<Status, CassRow> GetWarehouse_(uint32_t) noexcept;
  Status UpdateWarehouse_(uint32_t) noexcept;

  std::pair<Status, CassRow> GetCustomer_(uint32_t) noexcept;

  std::pair<Status, CassRow> GetDistrict_(uint32_t) noexcept;

  CassSession* conn_;
  static constexpr int MaxRetryCnt = 3;
};
}  // namespace ydb_util

#endif