#ifndef YCQL_RELATED_CUSTOMER_TXN_H_
#define YCQL_RELATED_CUSTOMER_TXN_H_
#include <unordered_map>
#include <vector>

#include "common/txn/related_customer_txn.h"

namespace ycql_impl {
class YCQLRelatedCustomerTxn : public ydb_util::RelatedCustomerTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLRelatedCustomerTxn(CassSession* session, std::ofstream& txn_out,
                                  std::ofstream& err_out)
      : RelatedCustomerTxn(),
        conn_(session),
        txn_out_(txn_out),
        err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  FRIEND_TEST(TxnArgsParserTest, related_customer);
  FRIEND_TEST(CQLTxnExecuteTest, RelatedCustomerTxnTest);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  constexpr static int MAX_RETRY_ATTEMPTS = 3;
  constexpr static int THRESHOLD = 2;

  Status executeLocal() noexcept;
  std::pair<Status, CassIterator*> getOrders() noexcept;
  std::pair<Status, CassIterator*> getOrderLines(int32_t o_id) noexcept;
  std::pair<Status, CassIterator*> getRelatedOrders(
      const std::vector<int32_t>& i_ids) noexcept;
  std::pair<Status, CassIterator*> getCustomerId(int32_t w_id, int32_t d_id,
                                                 int32_t o_id) noexcept;
  Status addRelatedCustomers(
      const std::vector<int32_t>& i_ids,
      std::unordered_map<int32_t, std::string>& customers) noexcept;
};
}  // namespace ycql_impl
#endif