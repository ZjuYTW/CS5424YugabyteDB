#ifndef YCQL_POPULAR_ITEM_TXN_H_
#define YCQL_POPULAR_ITEM_TXN_H_
#include "common/txn/popular_item_txn.h"
#include <unordered_map>

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
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  constexpr static int MaxRetryTime = 3;

  uint32_t getNextOrderId();
  std::pair<Status, CassIterator*> getLastOrders(uint32_t);
  CassIterator *getCustomerName(uint32_t);
  CassIterator *getMaxOrderLines(uint32_t);
  std::string getItemName(uint32_t);

};
}  // namespace ycql_impl

#endif