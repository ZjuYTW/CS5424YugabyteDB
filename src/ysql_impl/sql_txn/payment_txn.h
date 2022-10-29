#ifndef YSQL_PAYMENT_TXN_H_
#define YSQL_PAYMENT_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/payment_txn.h"

namespace ydb_util {
class YSQLPaymentTxn : public PaymentTxn {
 public:
  explicit YSQLPaymentTxn(pqxx::connection* conn, std::ofstream& txn_out,
                          std::ofstream& err_out) : PaymentTxn(), conn_(conn), txn_out_(txn_out), err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept;

 private:
  void updateWareHouseSQL_(int w_id, double old_w_ytd, double w_ytd,
                           pqxx::work* txn);

  void updateDistrictSQL_(int w_id, int d_id, double old_d_ytd, double d_ytd,
                          pqxx::work* txn);

  pqxx::row getDistrictSQL_(int w_id, int d_id, pqxx::work* txn);

  pqxx::row getWarehouseSQL_(int w_id, pqxx::work* txn);

  static constexpr int MAX_RETRY_COUNT = 6;
  pqxx::connection* conn_;
  std::vector<std::string> outputs;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ydb_util
#endif