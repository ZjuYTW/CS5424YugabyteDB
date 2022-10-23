#ifndef YSQL_POPULAR_ITEM_TXN_H_
#define YSQL_POPULAR_ITEM_TXN_H_
#include <pqxx/pqxx>

#include "common/txn/popular_item_txn.h"

namespace ydb_util {
class YSQLPopularItemTxn : public PopularItemTxn {
 public:
  explicit YSQLPopularItemTxn(pqxx::connection* conn)
      : PopularItemTxn(), conn_(conn) {}

  Status Execute() noexcept override;

 private:
  std::vector<std::string> outputs;
  static constexpr int MAX_RETRY_COUNT = 3;
  pqxx::connection* conn_;
};
}  // namespace ydb_util
#endif