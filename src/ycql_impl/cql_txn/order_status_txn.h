#ifndef YCQL_ORDER_STATUS_TXN_H_
#define YCQL_ORDER_STATUS_TXN_H_
#include "common/txn/order_status_txn.h"

namespace ycql_impl {
class YCQLOrderStatusTxn : public ydb_util::OrderStatusTxn {
  using Status = ydb_util::Status;

 public:
  YCQLOrderStatusTxn(CassSession* session, std::ofstream& txn_out,
                     std::ofstream& err_out)
      : OrderStatusTxn(),
        conn_(session),
        txn_out_(txn_out),
        err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override;

 private:
#ifdef BUILD_TEST_PERF
  FRIEND_TEST(TxnArgsParserTest, order_status);
  FRIEND_TEST(CQLTxnExecuteTest, OrderStatusTxnTest);
#endif
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  std::vector<std::string> outputs_;
  constexpr static int MAX_RETRY_ATTEMPTS = 3;

  Status executeLocal() noexcept;
  std::pair<Status, CassIterator*> getCustomerInfo() noexcept;
  std::pair<Status, CassIterator*> getLastOrder() noexcept;
  std::pair<Status, CassIterator*> getOrderLines(int32_t o_id) noexcept;
};
}  // namespace ycql_impl

#endif