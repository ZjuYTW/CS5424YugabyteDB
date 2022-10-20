#ifndef YSQL_IMPL_DELIVERY_TXN_H_
#define YSQL_IMPL_DELIVERY_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/delivery_txn.h"

namespace ydb_util {
class YSQLDeliveryTxn : public DeliveryTxn {
 public:
  explicit YSQLDeliveryTxn(pqxx::connection* conn)
      : DeliveryTxn(), conn_(conn) {}

  Status Execute() noexcept;

 private:
  static constexpr int MAX_RETRY_COUNT = 3;
  pqxx::connection* conn_;
};
}  // namespace ydb_util

#endif