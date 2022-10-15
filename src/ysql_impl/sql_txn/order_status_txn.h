#ifndef YSQL_ORDER_STATUS_TXN_H_
#define YSQL_ORDER_STATUS_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/order_status_txn.h"

namespace ydb_util {
class YSQLOrderStatusTxn : public OrderStatusTxn {
 public:
  explicit YSQLOrderStatusTxn(pqxx::connection* conn)
      : OrderStatusTxn(), conn_(conn) {}

  Status Execute() noexcept override { return Status::OK(); }

 private:
  pqxx::connection* conn_;
};
}  // namespace ydb_util
#endif