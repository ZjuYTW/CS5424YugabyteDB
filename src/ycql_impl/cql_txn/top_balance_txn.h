#ifndef YCQL_TOP_BALANCE_TXN_H_
#define YCQL_TOP_BALANCE_TXN_H_
#include "common/txn/top_balance_txn.h"

namespace ydb_util {
class YCQLTopBalanceTxn : public TopBalanceTxn {
 public:
  explicit YCQLTopBalanceTxn(CassSession* session)
      : TopBalanceTxn(), conn_(session) {}

  float Execute() noexcept override { return 0; }

 private:
  CassSession* conn_;
};
}  // namespace ydb_util
#endif