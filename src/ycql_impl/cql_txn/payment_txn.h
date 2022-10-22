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
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  FRIEND_TEST(TxnArgsParserTest, payment);
  static constexpr int MaxRetryTime = 3;

  Status updateWarehouseYTD();
  Status updateDistrictYTD();
  Status updateCustomerPayment();
  std::pair<Status, CassIterator *> getCustomer();
  std::pair<Status, CassIterator *> getWarehouse();
  std::pair<Status, CassIterator *> getDistrict();

};
}  // namespace ycql_impl

#endif