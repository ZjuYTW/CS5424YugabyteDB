#ifndef YDB_PERF_DELIVERY_TXN_H_
#define YDB_PERF_DELIVERY_TXN_H_

#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class DeliveryTxn : public Txn<Connection> {
 public:
  explicit DeliveryTxn(Connection* conn)
      : Txn<Connection>(TxnType::delivery, conn) {}

  Status ExecuteCQL() noexcept override;
  Status ExecuteSQL() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

}  // namespace ydb_util
#endif