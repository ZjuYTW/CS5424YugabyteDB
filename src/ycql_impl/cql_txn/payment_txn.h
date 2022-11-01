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
  FRIEND_TEST(TxnArgsParserTest, payment);
  FRIEND_TEST(CQLTxnExecuteTest, PaymentTxnTest);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  std::vector<std::string> outputs_;
  static constexpr int MAX_RETRY_ATTEMPTS = 3;

  Status executeLocal() noexcept;
  Status updateWarehouseYTD() noexcept;
  Status updateDistrictYTD() noexcept;
  Status updateCustomerPayment() noexcept;
  std::pair<Status, CassIterator*> getCustomer() noexcept;
  std::pair<Status, CassIterator*> getWarehouse() noexcept;
  std::pair<Status, CassIterator*> getDistrict() noexcept;
};
}  // namespace ycql_impl

#endif