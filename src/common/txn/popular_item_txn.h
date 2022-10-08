#ifndef YDB_PERF_POPULAR_ITEM_TXN_H_
#define YDB_PERF_POPULAR_ITEM_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class PopularItemTxn : public Txn<Connection> {
 public:
  explicit PopularItemTxn(Connection* conn)
      : Txn<Connection>(TxnType::popular_item, conn) {}

  Status ExecuteCQL() noexcept override { return Status::OK(); }

  Status ExecuteSQL() noexcept override { return Status::OK(); }

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    return Status::OK();
  }
};

}  // namespace ydb_util
#endif