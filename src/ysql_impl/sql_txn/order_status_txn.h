#ifndef YSQL_ORDER_STATUS_TXN_H_
#define YSQL_ORDER_STATUS_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/order_status_txn.h"

namespace ydb_util {
class YSQLOrderStatusTxn : public OrderStatusTxn {
 public:
  explicit YSQLOrderStatusTxn(pqxx::connection* conn)
      : OrderStatusTxn(), conn_(conn) {}

  float Execute() noexcept;

 private:
  void SQL_Output_Customer_Name(int c_w_id, int c_d_id, int c_id, pqxx::work* txn);
  int SQL_Get_Last_O_ID(int o_w_id, int o_d_id, int o_c_id, pqxx::work* txn);
  void SQL_Get_Item(int ol_w_id, int ol_d_id, int ol_o_id, pqxx::work* txn);

  pqxx::connection* conn_;
  static constexpr int MAX_RETRY_COUNT = 3;
};
}  // namespace ydb_util
#endif