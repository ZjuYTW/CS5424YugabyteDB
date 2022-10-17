#include "ycql_impl/cql_txn/payment_txn.h"

#include "ycql_impl/cql_exe_util.h"

namespace ydb_util {
Status YCQLPaymentTxn::Execute() noexcept {
  LOG_INFO << "Payment Transaction started";
  int retry = 0;
  return Status::OK();
}
}  // namespace ydb_util