#ifndef YCQL_NEW_ORDER_TXN_H_
#define YCQL_NEW_ORDER_TXN_H_
#include "common/txn/new_order_txn.h"
#include "common/util/trace_timer.h"

namespace ycql_impl {
class YCQLNewOrderTxn : public ydb_util::NewOrderTxn {
  using Status = ydb_util::Status;

 public:
  YCQLNewOrderTxn(CassSession* session, std::ofstream& txn_out,
                  std::ofstream& err_out)
      : NewOrderTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}
#ifndef NDEBUG
  void SetTraceTimer(ydb_util::TraceTimer* timer) noexcept override {
    trace_timer_ = timer;
  }
#endif

  virtual ~YCQLNewOrderTxn() = default;

  Status Execute(double* diff_t) noexcept override;

 private:
  struct OrderLine {
    int32_t i_id;  // OL_I_ID
    int32_t w_id;  // OL_W_ID
    int32_t quantity;
  };

  Status executeLocal(std::vector<OrderLine>& order_lines,
                      int all_local) noexcept;

  std::pair<Status, CassIterator*> getStock(int32_t item_id,
                                            int32_t w_id) noexcept;

  std::pair<Status, CassIterator*> getWarehouse() noexcept;

  std::pair<Status, CassIterator*> getDistrict() noexcept;

  std::pair<Status, CassIterator*> getCustomer() noexcept;

  std::pair<Status, CassIterator*> getItem(int32_t item_id) noexcept;

  Status updateNextOId(int32_t next_o_id, int32_t prev_next_o_id) noexcept;

  Status updateStock(int32_t adjusted_qty, int32_t prev_qty,
                     int32_t ordered_qty, int remote_cnt, int32_t w_id,
                     int32_t item_id) noexcept;

  // Return status and total_amount
  std::pair<Status, int64_t> processOrderLines(
      std::vector<OrderLine>& order_lines, int32_t next_o_id) noexcept;

  Status processOrder(int32_t next_o_id, int32_t order_num, int all_local,
                      std::string& current_time) noexcept;

  Status processOrderMaxQuantity(const std::vector<OrderLine>& order_lines,
                                 int32_t next_o_id, int64_t total_amount) noexcept;

  Status processOrderNonDelivery(int32_t next_o_id) noexcept;

  void processItemOutput(size_t start_idx, const OrderLine& ol,
                         int64_t item_amount, int32_t s_quantity,
                         const std::string& i_name) noexcept;

  void processOutput(CassIterator* customer_it, int64_t total_amount,
                     std::string& current_time, double discount, double w_tax,
                     double d_tax, int32_t o_id) noexcept;

  CassSession* conn_;

  std::ofstream& txn_out_;
  std::ofstream& err_out_;

  ydb_util::TraceTimer* trace_timer_{nullptr};

  std::vector<std::string> outputs_;
#ifdef BUILD_TEST_PERF
  FRIEND_TEST(CQLTxnExecuteTest, NewOrderTest1);
  FRIEND_TEST(TxnArgsParserTest, new_order);
#endif
  constexpr static int MaxRetryTime = 3;
};
}  // namespace ycql_impl

#endif