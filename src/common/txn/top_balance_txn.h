#ifndef YDB_PERF_TOP_BALANCE_TXN_H_
#define YDB_PERF_TOP_BALANCE_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
class TopBalanceTxn : public Txn {
 public:
  explicit TopBalanceTxn() : Txn(TxnType::top_balance) {}

  virtual ~TopBalanceTxn() = default;

  virtual Status Execute() noexcept override = 0;

  // TopBalance consists of one line with 1 values: T
  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 1) {
      return Status::AssertionFailed("Expect TopBalance has 1 args, but got " +
                                     std::to_string(ids.size()));
    }
    // ignore txn identification 'T'
    return Status::OK();
  }

 private:
  FRIEND_TEST(TxnArgsParserTest, top_balance);
};
}  // namespace ydb_util
#endif