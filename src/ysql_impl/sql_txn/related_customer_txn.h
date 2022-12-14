#ifndef YSQL_RELATED_CUSTOMER_TXN_H_
#define YSQL_RELATED_CUSTOMER_TXN_H_
#include <pqxx/pqxx>
#include <unordered_set>
#include <vector>

#include "common/txn/related_customer_txn.h"

namespace ydb_util {
class YSQLRelatedCustomerTxn : public RelatedCustomerTxn {
 public:
  explicit YSQLRelatedCustomerTxn(pqxx::connection* conn,
                                  std::ofstream& txn_out,
                                  std::ofstream& err_out)
      : RelatedCustomerTxn(),
        conn_(conn),
        txn_out_(txn_out),
        err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept;

 private:
  void addCustomerSQL_(int w_id, std::vector<int> items,
                       std::unordered_set<std::string>& customers,
                       pqxx::work* txn);
  pqxx::result getOrdersSQL_(int c_w_id, int c_d_id, int c_id, pqxx::work* txn);
  pqxx::result getOrderLineSQL_(std::string o_id, std::string o_d_id,
                                std::string o_w_id, pqxx::work* txn);
  int getCustomerIdSQL_(std::string w_id, std::string d_id, std::string o_id,
                        pqxx::work* txn);

  static constexpr int MAX_RETRY_COUNT = 6;
  static constexpr int INCOMMON_THRESHOLD = 2;
  std::vector<std::string> outputs;

  pqxx::connection* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ydb_util
#endif