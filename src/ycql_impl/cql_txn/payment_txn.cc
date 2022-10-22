#include "ycql_impl/cql_txn/payment_txn.h"

#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
Status YCQLPaymentTxn::Execute() noexcept {
  LOG_INFO << "Payment Transaction started";
  int retry = 0;
  auto st = Status::OK();
  while (retry++ < MaxRetryCnt) {
    // st = getWarehouse_(w_id_, );
  }
}
}  // namespace ycql_impl