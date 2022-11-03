#ifndef YCQL_PAYMENT_TXN_H_
#define YCQL_PAYMENT_TXN_H_
#include "common/txn/payment_txn.h"
#include "common/util/trace_timer.h"

namespace ycql_impl {
class YCQLPaymentTxn : public ydb_util::PaymentTxn {
  using Status = ydb_util::Status;

 public:
  YCQLPaymentTxn(CassSession* session, std::ofstream& txn_out,
                 std::ofstream& err_out)
      : PaymentTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}
  Status Execute(double* diff_t) noexcept override;

#ifndef NDEBUG
  void SetTraceTimer(ydb_util::TraceTimer* timer) noexcept override {
    trace_timer_ = timer;
  }
#endif

 private:
#ifdef BUILD_TEST_PERF
  FRIEND_TEST(TxnArgsParserTest, payment);
  FRIEND_TEST(CQLTxnExecuteTest, PaymentTxnTest);
#endif
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;

#ifndef NDEBUG
  ydb_util::TraceTimer* trace_timer_{nullptr};
#endif

  std::vector<std::string> outputs_;
  static constexpr int MAX_RETRY_ATTEMPTS = 3;

  Status executeLocal() noexcept;
  Status updateWarehouseYTD() noexcept;
  Status updateDistrictYTD() noexcept;
  Status updateCustomerPayment() noexcept;
  Status batchUpdateCustomerDistrictWarehouse() noexcept;
  std::pair<Status, CassIterator*> getCustomer() noexcept;
  std::pair<Status, CassIterator*> getWarehouse() noexcept;
  std::pair<Status, CassIterator*> getDistrict() noexcept;
};
}  // namespace ycql_impl

#endif