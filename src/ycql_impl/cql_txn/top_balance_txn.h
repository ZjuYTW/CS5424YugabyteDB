#ifndef YCQL_TOP_BALANCE_TXN_H_
#define YCQL_TOP_BALANCE_TXN_H_
#include <queue>

#include "common/txn/top_balance_txn.h"

namespace ycql_impl {
class YCQLTopBalanceTxn : public ydb_util::TopBalanceTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLTopBalanceTxn(CassSession* session, std::ofstream& txn_out,
                             std::ofstream& err_out)
      : TopBalanceTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  FRIEND_TEST(CQLTxnExecuteTest, TopBalanceTxnTest);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
  constexpr static int TOP_K = 10;
  constexpr static int MAX_RETRY_ATTEMPTS = 3;

  struct CustomerInfo {
    int64_t c_bal;
    int32_t c_w_id, c_d_id, c_id;
    bool operator<(const CustomerInfo& oth) const {
      return this->c_bal < oth.c_bal;
    }
    bool operator>(const CustomerInfo& oth) const {
      return this->c_bal > oth.c_bal;
    }
  };

  Status executeLocal() noexcept;
  std::pair<Status, CassIterator*> getAllCustomers() noexcept;
  std::pair<Status, CassIterator*> getCustomerName(
      const CustomerInfo& c_info) noexcept;
  std::pair<Status, CassIterator*> getWarehouse(int32_t w_id) noexcept;
  std::pair<Status, CassIterator*> getDistrict(int32_t w_id,
                                               int32_t d_id) noexcept;
};
}  // namespace ycql_impl
#endif