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

  // NewOrder starts with N, C_ID, W_ID, D_ID, M
  // and follows M lines
  Status Init(const std::string& first_line, std::ifstream& ifs) noexcept {
    auto ids = str_split(first_line, ',');
    assert(ids.size() == 5);
    uint32_t m;
    // ignore first N
    // C_ID
    c_id_ = stoi(ids[1]);
    // W_ID
    w_id_ = stoi(ids[2]);
    // D_ID
    d_id_ = stoi(ids[3]);
    // M
    m = stoi(ids[4]);
    std::string tmp;
    orders_.reserve(m);
    for (int i = 0; i < m; i++) {
      getline(ifs, tmp);
      orders_.push_back(tmp);
    }
  }

  Status ExecuteCQL() noexcept {
    CassError rc = CASS_OK;
    for (int i = 0; i < orders_.size(); i++) {
      uint32_t i_id, w_id, quantity;
      ParseOneOrder(orders_[i], &i_id, &w_id, &quantity);
      // Put your logic here, from Project New-Order Txn here
    }
  }

  Status ExecuteSQL() noexcept {}

 private:
  static Status ParseOneOrder(const std::string& order, uint32_t* i_id,
                              uint32_t* w_id, uint32_t* quantity) {}

  std::vector<std::string> orders_;
  // Maybe change it into BigInt
  uint32_t c_id_, w_id_, d_id_;
};
};  // namespace ydb_util

#endif