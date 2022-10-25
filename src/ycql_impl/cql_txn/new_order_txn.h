#ifndef YCQL_NEW_ORDER_TXN_H_
#define YCQL_NEW_ORDER_TXN_H_
#include "common/txn/new_order_txn.h"

namespace ycql_impl {
class YCQLNewOrderTxn : public ydb_util::NewOrderTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLNewOrderTxn(CassSession* session)
      : NewOrderTxn(), conn_(session) {}

  virtual ~YCQLNewOrderTxn() = default;

  Status Execute(double* diff_t) noexcept override;

 private:
  struct OrderLine {
    uint32_t i_id;  // OL_I_ID
    uint32_t w_id;  // OL_W_ID
    uint32_t quantity;
  };
  std::pair<Status, CassIterator*> getStock(uint32_t item_id,
                                            uint32_t w_id) noexcept;

  std::pair<Status, CassIterator*> getWarehouse() noexcept;

  std::pair<Status, CassIterator*> getDistrict() noexcept;

  std::pair<Status, CassIterator*> getCustomer() noexcept;

  std::pair<Status, CassIterator*> getItem(uint32_t item_id) noexcept;

  Status updateNextOId(uint32_t next_o_id, uint32_t prev_next_o_id) noexcept;

  Status updateStock(uint32_t adjusted_qty, uint32_t prev_qty,
                     uint32_t ordered_qty, int remote_cnt, uint32_t w_id,
                     uint32_t item_id) noexcept;

  // Return status and total_amount
  std::pair<Status, int64_t> processOrderLines(
      std::vector<OrderLine>& order_lines, uint32_t next_o_id) noexcept;

  Status processOrder(uint32_t next_o_id, uint32_t order_num, int all_local,
                      int64_t total_amount) noexcept;

  CassSession* conn_;

  FRIEND_TEST(CQLNewOrderTxnTest, NewOrderTest1);
  FRIEND_TEST(TxnArgsParserTest, new_order);
  constexpr static int MaxRetryTime = 3;
};
}  // namespace ycql_impl

#endif