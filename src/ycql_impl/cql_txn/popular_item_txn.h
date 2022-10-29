#ifndef YCQL_POPULAR_ITEM_TXN_H_
#define YCQL_POPULAR_ITEM_TXN_H_
#include <unordered_map>

#include "common/txn/popular_item_txn.h"

namespace ycql_impl {
class YCQLPopularItemTxn : public ydb_util::PopularItemTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLPopularItemTxn(CassSession* session, std::ofstream& txn_out,
                              std::ofstream& err_out)
      : PopularItemTxn(),
        conn_(session),
        txn_out_(txn_out),
        err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  FRIEND_TEST(TxnArgsParserTest, popular_item);
  FRIEND_TEST(CQLTxnExecuteTest, PopularItemTxnTest);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  constexpr static int MAX_RETRY_ATTEMPTS = 3;

  Status executeLocal() noexcept;
  std::pair<Status, CassIterator*> getNextOrder() noexcept;
  std::pair<Status, CassIterator*> getLastOrders(int32_t next_o_id) noexcept;
  std::pair<Status, CassIterator*> getCustomerName(int32_t c_id) noexcept;
  std::pair<Status, CassIterator*> getMaxOrderLines(int32_t o_id) noexcept;
  std::pair<Status, CassIterator*> getItemName(int32_t i_id) noexcept;
};
}  // namespace ycql_impl

#endif