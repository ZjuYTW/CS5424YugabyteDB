#ifndef YSQL_NEW_ORDER_TXN_H_
#define YSQL_NEW_ORDER_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/new_order_txn.h"
namespace ydb_util {
class YSQLNewOrderTxn : public NewOrderTxn {
 public:
  explicit YSQLNewOrderTxn(pqxx::connection* conn,std::ofstream& txn_out,std::ofstream& err_out)
      : NewOrderTxn(), conn_(conn),txn_out_(txn_out),err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept;

 private:
  int SQL_Get_D_Next_O_ID(int w_id, int d_id, pqxx::work* txn);
  void SQL_Update_D_Next_O_ID(int amount, int w_id, int d_id, pqxx::work* txn);
  void SQL_InsertNewOrder(int n, int allLocal, pqxx::work* txn);
  char* get_local_time(char* time_str, int len, struct timeval* tv);
  int SQL_Get_S_Quantity(int w_id, int i_id, pqxx::work* txn);
  void SQL_Update_S_Quantity(int w_id, int i_id, int s_quantity, int quantity,
                             int order_cnt, int remote_cnt, pqxx::work* txn);
  float SQL_Get_I_Price(int i_id, pqxx::work* txn);
  void SQL_InsertNewOrderLine(int n, int i, int item_number, float ol_amount,
                              int supply_w_id, int quantity, pqxx::work* txn);
  float SQL_Get_D_Tax(int w_id, int d_id, pqxx::work* txn);
  float SQL_Get_W_Tax(int w_id, pqxx::work* txn);
  float SQL_Get_C_Discount(int w_id, int d_id, int id, pqxx::work* txn);


  pqxx::connection* conn_;
  static constexpr int MAX_RETRY_COUNT = 3;
  std::vector<std::string> outputs;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;

};
}  // namespace ydb_util
#endif