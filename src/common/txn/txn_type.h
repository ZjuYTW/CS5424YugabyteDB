#ifndef YDB_PERF_TXN_TYPE_H_
#define YDB_PERF_TXN_TYPE_H_

#include <fstream>
#include <vector>

#include "cassandra.h"
#include "common/util/logger.h"
#include "common/util/status.h"
#include "common/util/string_util.h"
#include "gtest/gtest_prod.h"

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

class Txn {
 public:
  explicit Txn(TxnType type) : txn_type_(type) {}

  virtual ~Txn() = default;

  virtual Status Execute(double* diff_t) noexcept = 0;

  virtual Status Init(const std::string& first_line,
                      std::ifstream& ifs) noexcept = 0;

  inline TxnType GetTxnType() noexcept { return txn_type_; }

 private:
  TxnType txn_type_;
};

};  // namespace ydb_util

#endif