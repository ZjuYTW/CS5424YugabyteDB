#ifndef YSQL_PAYMENT_TXN_H_
#define YSQL_PAYMENT_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/payment_txn.h"

namespace ydb_util {
class YSQLPaymentTxn : public PaymentTxn {
 public:
  explicit YSQLPaymentTxn(pqxx::connection* conn) : PaymentTxn(), conn_(conn) {}

  float Execute() noexcept;

 private:
  void updateWareHouseSQL_(int w_id, double old_w_ytd, double w_ytd,
                           pqxx::work* txn);

  void updateDistrictSQL_(int w_id, int d_id, double old_d_ytd, double d_ytd,
                          pqxx::work* txn);

  pqxx::row getDistrictSQL_(int w_id, int d_id, pqxx::work* txn);

  pqxx::row getWarehouseSQL_(int w_id, pqxx::work* txn);

  static constexpr int MAX_RETRY_COUNT = 3;
  pqxx::connection* conn_;
  std::vector<std::string> outputs;
};
}  // namespace ydb_util
#endif