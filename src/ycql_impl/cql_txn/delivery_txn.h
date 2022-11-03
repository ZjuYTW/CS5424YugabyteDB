#ifndef YCQL_IMPL_DELIVERY_TXN_H_
#define YCQL_IMPL_DELIVERY_TXN_H_
#include <sys/_types/_int32_t.h>

#include "common/txn/delivery_txn.h"
#include "common/util/trace_timer.h"

namespace ycql_impl {
class YCQLDeliveryTxn : public ydb_util::DeliveryTxn {
  using Status = ydb_util::Status;

 public:
  YCQLDeliveryTxn(CassSession* session, std::ofstream& txn_out,
                  std::ofstream& err_out)
      : DeliveryTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}
#ifndef NDEBUG
  void SetTraceTimer(ydb_util::TraceTimer* timer) noexcept override {
    trace_timer_ = timer;
  }
#endif

  Status Execute(double* diff_t) noexcept override;

 private:
#ifdef BUILD_TEST_PERF
  FRIEND_TEST(TxnArgsParserTest, delivery);
  FRIEND_TEST(CQLTxnExecuteTest, DeliveryTxnTest);
#endif
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
#ifndef NDEBUG
  ydb_util::TraceTimer* trace_timer_{nullptr};
#endif
  constexpr static int MAX_RETRY_ATTEMPTS = 3;

  Status executeLocal(int32_t d_id) noexcept;
  std::pair<Status, CassIterator*> getNextDeliveryOrder(int32_t d_id) noexcept;
  std::pair<Status, CassIterator*> getOrderPaymentAmount(int32_t o_id,
                                                         int32_t d_id) noexcept;
  std::pair<Status, CassIterator*> getAllOrderLineNumber(int32_t o_id,
                                                         int32_t d_id) noexcept;
  Status updateCarrierId(int32_t o_id, int32_t d_id) noexcept;
  Status updateOrderLineDeliveryDate(int32_t o_id, int32_t d_id) noexcept;
  Status updateCustomerBalAndDeliveryCnt(int32_t c_id, int64_t total_amount,
                                         int32_t d_id) noexcept;
};
};  // namespace ycql_impl

#endif