#ifndef YSQL_TOP_BALANCE_TXN_H_
#define YSQL_TOP_BALANCE_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/top_balance_txn.h"

namespace ydb_util {
class YSQLTopBalanceTxn : public TopBalanceTxn {
 public:
  explicit YSQLTopBalanceTxn(pqxx::connection* conn)
      : TopBalanceTxn(), conn_(conn) {}

  Status Execute() noexcept override;

 private:
  pqxx::connection* conn_;
};
}  // namespace ydb_util
#endif