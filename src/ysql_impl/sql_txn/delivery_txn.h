#ifndef YSQL_IMPL_DELIVERY_TXN_H_
#define YSQL_IMPL_DELIVERY_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/delivery_txn.h"

namespace ydb_util {
class YSQLDeliveryTxn : public DeliveryTxn {
 public:
  explicit YSQLDeliveryTxn(pqxx::connection* conn)
      : DeliveryTxn(), conn_(conn) {}

  virtual ~YSQLDeliveryTxn() = default;

  Status Execute() noexcept override { return Status::OK(); }

 private:
  pqxx::connection* conn_;
};
}  // namespace ydb_util

#endif