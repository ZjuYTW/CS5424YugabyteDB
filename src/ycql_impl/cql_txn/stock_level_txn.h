#ifndef YCQL_STOCK_LEVEL_TXN_H_
#define YCQL_STOCK_LEVEL_TXN_H_
#include "common/txn/stock_level_txn.h"
#include "common/util/trace_timer.h"

namespace ycql_impl {
class YCQLStockLevelTxn : public ydb_util::StockLevelTxn {
  using Status = ydb_util::Status;

 public:
  YCQLStockLevelTxn(CassSession* session, std::ofstream& txn_out,
                    std::ofstream& err_out)
      : StockLevelTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}

#ifndef NDEBUG
  void SetTraceTimer(ydb_util::TraceTimer* timer) noexcept override {
    trace_timer_ = timer;
  }
#endif

  Status Execute(double* diff_t) noexcept override;

 private:
#ifdef BUILD_TEST_PERF
  FRIEND_TEST(TxnArgsParserTest, stock_level);
  FRIEND_TEST(CQLTxnExecuteTest, StockLevelTxnTest);
#endif
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
#ifndef NDEBUG
  ydb_util::TraceTimer* trace_timer_{nullptr};
#endif
  std::vector<std::string> outputs_;
  constexpr static int MAX_RETRY_ATTEMPTS = 3;

  Status executeLocal() noexcept;
  std::pair<Status, CassIterator*> getNextOrder(
      const CassResult** result) noexcept;
  std::pair<Status, CassIterator*> getItemsInLastOrders(
      int32_t next_o_id, const CassResult** result) noexcept;
  std::pair<Status, CassIterator*> getItemQuantityFromStock(
      int32_t i_id, const CassResult** result) noexcept;
};
}  // namespace ycql_impl
#endif