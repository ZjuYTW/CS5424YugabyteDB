#include "ycql_impl/cql_txn/delivery_txn.h"
#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
Status YCQLDeliveryTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Delivery Transaction started";
  Status st;

  for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
    int retry_time = 0;
    CassIterator *order_it = nullptr;
    CassIterator *amount_it = nullptr;
    bool done = false;
    do {
      order_it = getNextDeliveryOrder(d_id);
      auto o_id = GetValueFromCassRow<uint32_t>(order_it, "o_id");
      auto c_id = GetValueFromCassRow<uint32_t>(order_it, "o_c_id");

      st = updateCarrierId(d_id, o_id);
      if (!st.ok()) continue;

      st = updateOrderLineDeliveryDate(d_id, o_id);
      if (!st.ok()) continue;

      amount_it = getOrderPaymentAmount(d_id, o_id);
      auto total_amount = GetValueFromCassRow<uint32_t>(amount_it, "sum_ol_amount");

      st = updateCustomerBalAndDeliveryCnt(d_id, c_id, total_amount);
      if (!st.ok()) continue;

    } while (retry_time++ < MaxRetryTime /* && sleep(done) */);
    if (order_it) cass_iterator_free(order_it);
    if (amount_it) cass_iterator_free(amount_it);
  }
  return st;
}

CassIterator *YCQLDeliveryTxn::getNextDeliveryOrder(uint32_t d_id) {
  std::string stmt =
      "SELECT o_id, o_c_id "
      "FROM orders "
      "WHERE o_w_id = ? AND o_d_id = ? AND O_CARRIER_ID IS NULL "
      "ORDER BY o_id ASC"
      "LIMIT 1"
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  // TODO(winston.yan): handle error
  auto _ = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id);
  return it;
}

Status YCQLDeliveryTxn::updateCarrierId(uint32_t d_id, uint32_t o_id) {
  std::string stmt =
      "UPDATE orders "
      "SET o_carrier_id = ? "
      "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?;";
  CassIterator* it = nullptr;
  return ycql_impl::execute_write_cql(conn_, stmt, &it, carrier_id_, w_id_, d_id, o_id);
}

Status YCQLDeliveryTxn::updateOrderLineDeliveryDate(uint32_t d_id, uint32_t o_id) {
  std::string stmt =
      "UPDATE orderline "
      "SET ol_delivery_d = currenttimestamp() "
      "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
  CassIterator* it = nullptr;
  return ycql_impl::execute_write_cql(conn_, stmt, &it, w_id_, d_id, o_id);
}

CassIterator *YCQLDeliveryTxn::getOrderPaymentAmount(uint32_t d_id, uint32_t o_id) {
  std::string stmt =
      "SELECT SUM(ol_amount) as sum_ol_amount "
      "FROM orderline "
      "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  // TODO(winston.yan): handle error
  auto _ = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id, o_id);
  return it;
}

Status YCQLDeliveryTxn::updateCustomerBalAndDeliveryCnt(uint32_t d_id, uint32_t c_id, uint32_t total_amount) {
  std::string stmt =
      "UPDATE customer "
      "SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
  CassIterator* it = nullptr;
  return ycql_impl::execute_write_cql(conn_, stmt, &it, total_amount, w_id_, d_id, c_id);

}



};  // namespace ycql_impl