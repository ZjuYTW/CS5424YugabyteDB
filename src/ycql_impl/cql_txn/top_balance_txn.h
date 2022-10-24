#ifndef YCQL_TOP_BALANCE_TXN_H_
#define YCQL_TOP_BALANCE_TXN_H_
#include "common/txn/top_balance_txn.h"

namespace ycql_impl {
class YCQLTopBalanceTxn : public ydb_util::TopBalanceTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLTopBalanceTxn(CassSession* session)
      : TopBalanceTxn(), conn_(session) {}

  Status Execute(double* diff_t) noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
};
}  // namespace ycql_impl
#endif