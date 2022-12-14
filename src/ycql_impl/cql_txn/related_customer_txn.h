#ifndef YCQL_RELATED_CUSTOMER_TXN_H_
#define YCQL_RELATED_CUSTOMER_TXN_H_
#include <unordered_map>
#include <vector>

#include "common/txn/related_customer_txn.h"
#include "common/util/trace_timer.h"

namespace ycql_impl {
class YCQLRelatedCustomerTxn : public ydb_util::RelatedCustomerTxn {
  using Status = ydb_util::Status;

 public:
  YCQLRelatedCustomerTxn(CassSession* session, std::ofstream& txn_out,
                         std::ofstream& err_out)
      : RelatedCustomerTxn(),
        conn_(session),
        txn_out_(txn_out),
        err_out_(err_out) {}

#ifndef NDEBUG
  void SetTraceTimer(ydb_util::TraceTimer* timer) noexcept override {
    trace_timer_ = timer;
  }
#endif

  Status Execute(double* diff_t) noexcept override;

 private:
#ifdef BUILD_TEST_PERF
  FRIEND_TEST(TxnArgsParserTest, related_customer);
  FRIEND_TEST(CQLTxnExecuteTest, RelatedCustomerTxnTest);
#endif
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
#ifndef NDEBUG
  ydb_util::TraceTimer* trace_timer_{nullptr};
#endif
  std::vector<std::string> outputs_;
  constexpr static int MAX_RETRY_ATTEMPTS = 3;
  constexpr static int THRESHOLD = 2;

  typedef std::tuple<int32_t, int32_t, int32_t> order_key_t;
  struct order_key_hash : public std::unary_function<order_key_t, std::size_t> {
    std::size_t operator()(const order_key_t& k) const;
  };

  Status executeLocal() noexcept;
  std::pair<Status, CassIterator*> getOrders(
      const CassResult** result) noexcept;
  std::pair<Status, CassIterator*> getOrderLines(
      int32_t o_id, const CassResult** result) noexcept;
  std::pair<Status, CassIterator*> getRelatedOrders(
      const std::vector<int32_t>& i_ids, const int32_t w_id,
      const CassResult** result) noexcept;
  std::pair<Status, CassIterator*> getCustomerId(
      int32_t w_id, int32_t d_id, int32_t o_id,
      const CassResult** result) noexcept;
  static std::string idSetToString(const std::vector<int32_t>& i_ids) noexcept;
  Status addRelatedCustomers(
      const std::vector<int32_t>& i_ids,
      std::unordered_map<int32_t, std::string>& customers) noexcept;
};
}  // namespace ycql_impl
#endif