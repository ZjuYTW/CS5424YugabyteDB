#ifndef YCQL_STOCK_LEVEL_TXN_H_
#define YCQL_STOCK_LEVEL_TXN_H_
#include "common/txn/stock_level_txn.h"

namespace ycql_impl {
class YCQLStockLevelTxn : public ydb_util::StockLevelTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLStockLevelTxn(CassSession* session, std::ofstream& txn_out,
                             std::ofstream& err_out)
      : StockLevelTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  FRIEND_TEST(TxnArgsParserTest, stock_level);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  constexpr static int MAX_RETRY_ATTEMPTS = 3;

  Status execute() noexcept;
  std::pair<Status, CassIterator*> getNextOrder() noexcept;
  std::pair<Status, CassIterator*> getItemsInLastOrders(int32_t next_o_id) noexcept;
  std::pair<Status, CassIterator*> getItemQuantityFromStock(int32_t) noexcept;
};
}  // namespace ycql_impl
#endif