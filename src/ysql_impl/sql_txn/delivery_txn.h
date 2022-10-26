#ifndef YSQL_IMPL_DELIVERY_TXN_H_
#define YSQL_IMPL_DELIVERY_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/delivery_txn.h"

namespace ydb_util {
class YSQLDeliveryTxn : public DeliveryTxn {
 public:
  explicit YSQLDeliveryTxn(pqxx::connection* conn, std::ofstream& txn_out,
                           std::ofstream& err_out)
      : DeliveryTxn(), conn_(conn), txn_out_(txn_out), err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept;

 private:
  static constexpr int MAX_RETRY_COUNT = 3;
  pqxx::connection* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ydb_util

#endif