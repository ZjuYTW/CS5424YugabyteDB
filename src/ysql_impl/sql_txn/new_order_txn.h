#ifndef YSQL_NEW_ORDER_TXN_H_
#define YSQL_NEW_ORDER_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/new_order_txn.h"
namespace ydb_util {
class YSQLNewOrderTxn : public NewOrderTxn {
 public:
  explicit YSQLNewOrderTxn(pqxx::connection* conn)
      : NewOrderTxn(), conn_(conn) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  pqxx::connection* conn_;
};
}  // namespace ydb_util
#endif