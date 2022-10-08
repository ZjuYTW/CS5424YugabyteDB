#ifndef YDB_PERF_POPULAR_ITEM_TXN_H_
#define YDB_PERF_POPULAR_ITEM_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class PopularItemTxn : public Txn<Connection> {
 public:
  explicit PopularItemTxn(Connection* conn)
      : Txn<Connection>(TxnType::popular_item, conn) {}

  Status ExecuteCQL() noexcept override;

  Status ExecuteSQL() noexcept override;

  // PopularItem consists of one line with 4 values: I, W_ID, D_ID, L
  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 4) {
      return Status::AssertionFailed(
          "Expect PopularItem has 4 args, but got " +
          std::to_string(ids.size()));
    }
    // ignore txn identification 'I'
    w_id_ = stoi(ids[1]);
    d_id_ = stoi(ids[2]);
    l_ = stoi(ids[3]);
    return Status::OK();
  }

 private:
  uint32_t w_id_, d_id_, l_;
};
}  // namespace ydb_util
#endif