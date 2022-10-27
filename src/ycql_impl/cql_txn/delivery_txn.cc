#include "ycql_impl/cql_txn/delivery_txn.h"

#include <cassert>

#include "cassandra.h"
#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;

Status YCQLDeliveryTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Delivery transaction started";
  Status st = Status::OK();
  for (d_id_ = 1; d_id_ <= 10; ++d_id_) {
    st = Retry(std::bind(&YCQLDeliveryTxn::executeLocal, this),
               MAX_RETRY_ATTEMPTS);
    if (!st.ok()) {
      LOG_FATAL << "Delivery transaction execution failed"
                << ", " << st.ToString();
      if (st.isEndOfFile()) {
        // If just can't find an availiable order, skip
        continue;
      }
      return st;
    }
  }
  if (st.ok() || st.isEndOfFile()) {
    LOG_INFO << "Delivery transaction completed";
    return Status::OK();
  }
  return st;
}

Status YCQLDeliveryTxn::executeLocal() noexcept {
  Status st = Status::OK();

  CassIterator* order_it = nullptr;
  LOG_DEBUG << "Get Next Delivery Order";
  std::tie(st, order_it) = getNextDeliveryOrder();
  if (!st.ok()) return st;
  auto o_id = GetValueFromCassRow<int32_t>(order_it, "o_id");
  auto c_id = GetValueFromCassRow<int32_t>(order_it, "o_c_id");
  if (order_it) cass_iterator_free(order_it);

  LOG_DEBUG << "Update Carrier Id";
  st = updateCarrierId(o_id);
  if (!st.ok()) return st;

  LOG_DEBUG << "Update Order Line Delivery Date";
  st = updateOrderLineDeliveryDate(o_id);
  if (!st.ok()) return st;

  CassIterator* amount_it = nullptr;
  LOG_DEBUG << "Get OrderPaymentAmount";
  std::tie(st, amount_it) = getOrderPaymentAmount(o_id);
  if (!st.ok()) return st;
  auto total_amount = GetValueFromCassRow<int32_t>(amount_it, "sum_ol_amount");
  if (amount_it) cass_iterator_free(amount_it);

  LOG_DEBUG << "update Customer Bal And Delivery Cnt";
  st = updateCustomerBalAndDeliveryCnt(c_id, total_amount);
  if (!st.ok()) return st;

  return st;
}

std::pair<Status, CassIterator*>
YCQLDeliveryTxn::getNextDeliveryOrder() noexcept {
  std::string stmt =
      "SELECT o_id, o_c_id "
      "FROM " +
      YCQLKeyspace +
      ".orders "
      "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = NULL "
      "ORDER BY o_id ASC "
      "LIMIT 1 "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_);
  if (!st.ok()) {
    assert(!it);
    LOG_DEBUG << st.ToString();
    return {st, it};
  }
  if (!cass_iterator_next(it)) {
    cass_iterator_free(it);
    // This means we can't find a coressponding avaliable order, just skip it
    return {Status::EndOfFile("Next Delivery Order not found"), it};
  }
  return {st, it};
}

Status YCQLDeliveryTxn::updateCarrierId(int32_t o_id) noexcept {
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".orders "
                     "SET o_carrier_id = ? "
                     "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? "
                     ";";
  return ycql_impl::execute_write_cql(conn_, stmt, carrier_id_, w_id_, d_id_,
                                      o_id);
}

Status YCQLDeliveryTxn::updateOrderLineDeliveryDate(int32_t o_id) noexcept {
  // TODO(ZjuYTW): Batch Update here
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".orderline "
                     "SET ol_delivery_d = currenttimestamp() "
                     "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? "
                     ";";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id_, o_id);
}

std::pair<Status, CassIterator*> YCQLDeliveryTxn::getOrderPaymentAmount(
    int32_t o_id) noexcept {
  std::string stmt =
      "SELECT SUM(ol_amount) as sum_ol_amount "
      "FROM " +
      YCQLKeyspace +
      ".orderline "
      "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_, o_id);
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Order Payment Amount not found"), it};
  }
  return {st, it};
}

Status YCQLDeliveryTxn::updateCustomerBalAndDeliveryCnt(
    int32_t c_id, int32_t total_amount) noexcept {
  std::string stmt =
      "UPDATE " + YCQLKeyspace +
      ".customer "
      "SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
  return ycql_impl::execute_write_cql(conn_, stmt, total_amount, w_id_, d_id_,
                                      c_id);
}

};  // namespace ycql_impl