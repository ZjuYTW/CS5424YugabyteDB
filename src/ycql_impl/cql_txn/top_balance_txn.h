#ifndef YCQL_TOP_BALANCE_TXN_H_
#define YCQL_TOP_BALANCE_TXN_H_
#include "common/txn/top_balance_txn.h"

namespace ycql_impl {
class YCQLTopBalanceTxn : public ydb_util::TopBalanceTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLTopBalanceTxn(CassSession* session, std::ofstream& txn_out,
                             std::ofstream& err_out)
      : TopBalanceTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ycql_impl
#endif