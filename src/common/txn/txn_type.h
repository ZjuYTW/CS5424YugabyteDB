#ifndef YDB_PERF_TXN_TYPE_H_
#define YDB_PERF_TXN_TYPE_H_

#include <fstream>
#include <pqxx/pqxx>
#include <vector>

#include "cassandra.h"
#include "common/util/status.h"
#include "common/util/string_util.h"

namespace ydb_util {
enum class TxnType {
  new_order,
  payment,
  delivery,
  order_status,
  stock_level,
  popular_item,
  top_balance,
  related_customer,
};

template <typename Connection>
class Txn {
 public:
  explicit Txn(TxnType type, Connection* conn) : txn_type_(type), conn_(conn) {}

  virtual Status ExecuteCQL() noexcept = 0;
  virtual Status ExecuteSQL() noexcept = 0;

  virtual Status Init(const std::string& first_line,
                      std::ifstream& ifs) noexcept = 0;

  inline TxnType GetTxnType() noexcept { return txn_type_; }

  inline Connection* GetConnection() noexcept { return conn_; }

 private:
  TxnType txn_type_;
  Connection* conn_;
};

};  // namespace ydb_util

#endif