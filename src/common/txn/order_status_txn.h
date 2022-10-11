#ifndef YDB_PERF_ORDER_STATUS_TXN_H_
#define YDB_PERF_ORDER_STATUS_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class OrderStatusTxn : public Txn<Connection> {
 public:
  explicit OrderStatusTxn(Connection* conn)
      : Txn<Connection>(TxnType::order_status, conn) {}

  Status ExecuteCQL() noexcept override { return Status::OK(); }
  Status ExecuteSQL() noexcept override { return Status::OK(); }

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 4) {
      return Status::AssertionFailed("Expect DeliveryTxn has 4 args, but got " +
                                     std::to_string(ids.size()));
    }
    // ignore txn identification 'O'
    c_w_id_ = stoi(ids[1]);
    c_d_id_ = stoi(ids[2]);
    c_id_ = stoi(ids[3]);
    return Status::OK();
  }

 private:
  uint32_t c_w_id_, c_d_id_, c_id_;
  FRIEND_TEST(TxnArgsParserTest, order_status);
};

}  // namespace ydb_util
#endif