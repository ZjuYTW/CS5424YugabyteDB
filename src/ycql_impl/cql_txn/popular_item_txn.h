#ifndef YCQL_POPULAR_ITEM_TXN_H_
#define YCQL_POPULAR_ITEM_TXN_H_
#include "common/txn/popular_item_txn.h"

namespace ycql_impl {
class YCQLPopularItemTxn : public ydb_util::PopularItemTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLPopularItemTxn(CassSession* session)
      : PopularItemTxn(), conn_(session) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
};
}  // namespace ycql_impl

#endif