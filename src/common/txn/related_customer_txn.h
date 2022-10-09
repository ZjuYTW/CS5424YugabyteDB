#ifndef YDB_PERF_RELATED_CUSTOMER_TXN_H_
#define YDB_PERF_RELATED_CUSTOMER_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class RelatedCustomerTxn : public Txn<Connection> {
FRIEND_TEST(TxnArgsParserTest, related_customer);
 public:
  explicit RelatedCustomerTxn(Connection* conn)
      : Txn<Connection>(TxnType::related_customer, conn) {}

  Status ExecuteCQL() noexcept override {
    return Status::OK();
  }

  Status ExecuteSQL() noexcept override {
    return Status::OK();
  }

  // RelatedCustomer consists of one line with 4 values: R, C_W_ID, C_D_ID, C_ID
  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 4) {
      return Status::AssertionFailed(
          "Expect RelatedCustomer has 4 args, but got " +
          std::to_string(ids.size()));
    }
    // ignore txn identification 'R'
    c_w_id_ = stoi(ids[1]);
    c_d_id_ = stoi(ids[2]);
    c_id_ = stoi(ids[3]);
    return Status::OK();
  }

 private:
  uint32_t c_w_id_, c_d_id_, c_id_;
};

}  // namespace ydb_util
#endif