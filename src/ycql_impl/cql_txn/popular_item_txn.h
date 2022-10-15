#ifndef YCQL_POPULAR_ITEM_TXN_H_
#define YCQL_POPULAR_ITEM_TXN_H_
#include "common/txn/popular_item_txn.h"

namespace ydb_util {
class YCQLPopularItemTxn : public PopularItemTxn {
 public:
  explicit YCQLPopularItemTxn(CassSession* session)
      : PopularItemTxn(), conn_(session) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  CassSession* conn_;
};
}  // namespace ydb_util

#endif