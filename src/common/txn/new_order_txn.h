#ifndef YDB_PERF_NEW_ORDER_TXN_H_
#define YDB_PERF_NEW_ORDER_TXN_H_
#include <vector>

#include "common/txn/txn_type.h"

namespace ydb_util {

template <typename Connection>
class NewOrderTxn : public Txn<Connection> {
 public:
  explicit NewOrderTxn(Connection* conn)
      : Txn<Connection>(TxnType::new_order, conn) {}

  // NewOrder starts with 5 values: N, C_ID, W_ID, D_ID, M
  // and follows M lines, each line consists of order details
  Status Init(const std::string& first_line, std::ifstream& ifs) noexcept {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 5) {
      return Status::AssertionFailed(
          "Expect NewOrderTxn has 5 first line args, but got " +
          std::to_string(ids.size()));
    }
    uint32_t m;
    // ignore txn identification 'N'
    c_id_ = stoi(ids[1]);
    w_id_ = stoi(ids[2]);
    d_id_ = stoi(ids[3]);
    // M lines of order details
    m = stoi(ids[4]);
    std::string tmp;
    orders_.reserve(m);
    for (int i = 0; i < m; i++) {
      getline(ifs, tmp);
      orders_.push_back(tmp);
    }
    return Status::OK();
  }

  Status ExecuteCQL() noexcept {
    CassError rc = CASS_OK;
    for (int i = 0; i < orders_.size(); i++) {
      uint32_t i_id, w_id, quantity;
      ParseOneOrder(orders_[i], &i_id, &w_id, &quantity);
      // Put your logic here, from Project New-Order Txn here
    }
    return Status::OK();
  }

  Status ExecuteSQL() noexcept { return Status::OK(); }

 private:
  // ParseOneOrder parses each line into 3 values: OL_I_ID, OL_W_ID, OL_QUALITY
  static Status ParseOneOrder(const std::string& order, uint32_t* i_id,
                              uint32_t* w_id, uint32_t* quantity) {
    auto ret = str_split(order, ',');
    if (ret.size() != 3) {
      return Status::AssertionFailed(
          "Expect one order line has 3 args, but got " +
          std::to_string(ret.size()));
    }
    *i_id = stoi(ret[0]);
    *w_id = stoi(ret[1]);
    *quantity = stoi(ret[2]);
    return Status::OK();
  }

  std::vector<std::string> orders_;
  // Maybe change it into BigInt
  uint32_t c_id_, w_id_, d_id_;
  FRIEND_TEST(TxnArgsParserTest, new_order);
};

}  // namespace ydb_util

#endif