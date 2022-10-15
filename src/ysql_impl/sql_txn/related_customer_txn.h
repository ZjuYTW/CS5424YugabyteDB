#ifndef YSQL_RELATED_CUSTOMER_TXN_H_
#define YSQL_RELATED_CUSTOMER_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/related_customer_txn.h"

namespace ydb_util {
class YSQLRelatedCustomerTxn : public RelatedCustomerTxn {
 public:
  explicit YSQLRelatedCustomerTxn(pqxx::connection* conn)
      : RelatedCustomerTxn(), conn_(conn) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  pqxx::connection* conn_;
};
}  // namespace ydb_util
#endif