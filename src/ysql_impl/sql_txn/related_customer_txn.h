#ifndef YSQL_RELATED_CUSTOMER_TXN_H_
#define YSQL_RELATED_CUSTOMER_TXN_H_
#include <pqxx/pqxx>
#include <unordered_set>
#include <vector>

#include "common/txn/related_customer_txn.h"

namespace ydb_util {
class YSQLRelatedCustomerTxn : public RelatedCustomerTxn {
 public:
  explicit YSQLRelatedCustomerTxn(pqxx::connection* conn)
      : RelatedCustomerTxn(), conn_(conn) {}

  Status Execute(double* diff_t) noexcept;

 private:
  void addCustomerSQL_(int w_id, std::vector<int> items,
                       std::unordered_set<std::string>& customers,
                       pqxx::work* txn);
  pqxx::result getOrdersSQL_(int c_w_id, int c_d_id, int c_id, pqxx::work* txn);
  pqxx::result getOrderLineSQL_(int o_w_id, int o_d_id, int o_id,
                                pqxx::work* txn);
  int getCustomerIdSQL_(int w_id, int d_id, int o_id, pqxx::work* txn);

  static constexpr int MAX_RETRY_COUNT = 3;
  static constexpr int INCOMMON_THRESHOLD = 2;
  std::vector<std::string> outputs;

  pqxx::connection* conn_;
};
}  // namespace ydb_util
#endif