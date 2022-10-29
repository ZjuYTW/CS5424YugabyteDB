#ifndef YCQL_PAYMENT_TXN_H_
#define YCQL_PAYMENT_TXN_H_
#include "common/txn/payment_txn.h"

namespace ycql_impl {
class YCQLPaymentTxn : public ydb_util::PaymentTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLPaymentTxn(CassSession* session, std::ofstream& txn_out,
                          std::ofstream& err_out)
      : PaymentTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}
  Status Execute(double* diff_t) noexcept override;

 private:
  std::pair<Status, CassRow> GetWarehouse_(uint32_t) noexcept;
  Status UpdateWarehouse_(uint32_t) noexcept;

  std::pair<Status, CassRow> GetCustomer_(uint32_t) noexcept;

  std::pair<Status, CassRow> GetDistrict_(uint32_t) noexcept;

  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  FRIEND_TEST(TxnArgsParserTest, payment);
  static constexpr int MaxRetryCnt = 3;
};
}  // namespace ycql_impl

#endif