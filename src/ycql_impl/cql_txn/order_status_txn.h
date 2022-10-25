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
  FRIEND_TEST(TxnArgsParserTest, order_status);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  constexpr static int MaxRetryTime = 3;

  CassIterator *getCustomerInfo();
  CassIterator *getLastOrder();
  std::pair<Status, CassIterator*> getOrderLines(uint32_t o_id);



};
}  // namespace ycql_impl

#endif