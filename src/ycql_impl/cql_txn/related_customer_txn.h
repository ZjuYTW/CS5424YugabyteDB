#ifndef YCQL_RELATED_CUSTOMER_TXN_H_
#define YCQL_RELATED_CUSTOMER_TXN_H_
#include "common/txn/related_customer_txn.h"

namespace ydb_util {
class YCQLRelatedCustomerTxn : public RelatedCustomerTxn {
 public:
  explicit YCQLRelatedCustomerTxn(CassSession* session)
      : RelatedCustomerTxn(), conn_(session) {}

  float Execute() noexcept override { return 0; }

 private:
  CassSession* conn_;
};
}  // namespace ydb_util
#endif