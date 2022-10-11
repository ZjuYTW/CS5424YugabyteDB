#ifndef YDB_PERF_STOCK_LEVEL_TXN_H_
#define YDB_PERF_STOCK_LEVEL_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class StockLevelTxn : public Txn<Connection> {
 public:
  explicit StockLevelTxn(Connection* conn)
      : Txn<Connection>(TxnType::stock_level, conn) {}

  Status ExecuteCQL() noexcept override { return Status::OK(); }
  Status ExecuteSQL() noexcept override { return Status::OK(); }

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 5) {
      return Status::AssertionFailed("Expect DeliveryTxn has 5 args, but got " +
                                     std::to_string(ids.size()));
    }
    // ignore txn identification 'S'
    w_id_ = stoi(ids[1]);
    d_id_ = stoi(ids[2]);
    t_ = stoi(ids[3]);
    l_ = stoi(ids[4]);
    return Status::OK();
  }

 private:
  uint32_t w_id_, d_id_, t_, l_;
  FRIEND_TEST(TxnArgsParserTest, stock_level);
};

}  // namespace ydb_util
#endif