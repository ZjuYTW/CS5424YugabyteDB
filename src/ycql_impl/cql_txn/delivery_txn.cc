#include "ycql_impl/cql_txn/delivery_txn.h"


namespace ycql_impl {
using Status = ydb_util::Status;
Status YCQLDeliveryTxn::Execute(double* diff_t) noexcept { return Status::OK(); }
};  // namespace ycql_impl
