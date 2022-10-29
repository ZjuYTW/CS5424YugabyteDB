#ifndef YCQL_POPULAR_ITEM_TXN_H_
#define YCQL_POPULAR_ITEM_TXN_H_
#include "common/txn/popular_item_txn.h"

namespace ycql_impl {
class YCQLPopularItemTxn : public ydb_util::PopularItemTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLPopularItemTxn(CassSession* session, std::ofstream& txn_out,
                              std::ofstream& err_out)
      : PopularItemTxn(),
        conn_(session),
        txn_out_(txn_out),
        err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override { return Status::OK(); }

 private:
  FRIEND_TEST(TxnArgsParserTest, popular_item);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ycql_impl

#endif