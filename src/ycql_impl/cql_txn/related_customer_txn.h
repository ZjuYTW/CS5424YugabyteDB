#ifndef YCQL_RELATED_CUSTOMER_TXN_H_
#define YCQL_RELATED_CUSTOMER_TXN_H_
#include "common/txn/related_customer_txn.h"

namespace ycql_impl {
class YCQLRelatedCustomerTxn : public ydb_util::RelatedCustomerTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLRelatedCustomerTxn(CassSession* session, std::ofstream& txn_out,
                                  std::ofstream& err_out)
      : RelatedCustomerTxn(),
        conn_(session),
        txn_out_(txn_out),
        err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override { return Status::OK(); }

 private:
  FRIEND_TEST(TxnArgsParserTest, related_customer);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ycql_impl
#endif