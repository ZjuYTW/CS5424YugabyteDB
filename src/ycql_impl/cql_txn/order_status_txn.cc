#include "ycql_impl/cql_txn/order_status_txn.h"

#include "ycql_impl/cql_exe_util.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLOrderStatusTxn::Execute(double* diff_t) noexcept {
  if (YDB_SKIP_ORDER_STATUS) {
    *diff_t = 0;
    return Status::OK();
  }
#ifndef NDEBUG
  if (trace_timer_) {
    trace_timer_->Reset();
  }
#endif
  LOG_INFO << "Order-status Transaction started";
  const auto InputString = format("O %d %d %d", c_w_id_, c_d_id_, c_id_);
  auto start_time = std::chrono::system_clock::now();
  auto st = Retry(std::bind(&YCQLOrderStatusTxn::executeLocal, this),
                  MAX_RETRY_ATTEMPTS);
  auto end_time = std::chrono::system_clock::now();
  *diff_t = (end_time - start_time).count();
  if (st.ok()) {
    LOG_INFO << "Order-status Transaction completed, time cost " << *diff_t;
    // Txn output
    txn_out_ << InputString << std::endl;
    for (const auto& ostr : outputs_) {
      txn_out_ << "\t" << ostr << std::endl;
    }
  } else {
    err_out_ << InputString << std::endl;
    err_out_ << st.ToString() << std::endl;
  }
  return st;
}

Status YCQLOrderStatusTxn::executeLocal() noexcept {
  Status st = Status::OK();
  CassIterator *customer_it = nullptr, *order_it = nullptr,
               *orderLine_it = nullptr;

  std::tie(st, customer_it) = getCustomerInfo();
  if (!st.ok()) return st;

  outputs_.push_back(format(
      "(a) Customer name & balance: (C_FIRST,C_MIDDLE,C_LAST)=(%s, %s, %s), "
      "C_BALANCE: %s",
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_first"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_middle"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_last"))
          .c_str(),
      GetStringValue(
          GetValueFromCassRow<std::int64_t>(customer_it, "c_balance"), 100)
          .c_str()));

  LOG_DEBUG << "Get Last Order";
  std::tie(st, order_it) = getLastOrder();
  if (!st.ok()) return st;

  auto o_id = GetValueFromCassRow<int32_t>(order_it, "o_id").value();
  outputs_.push_back(format(
      "(b) O_ID: %d, O_ENTRY_D: %s, O_CARRIER_ID: %s", o_id,
      GetStringValue(GetValueFromCassRow<int64_t>(order_it, "o_entry_d"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<int32_t>(order_it, "o_carrier_id"))
          .c_str()));

  std::tie(st, orderLine_it) = getOrderLines(o_id);
  if (!st.ok()) return st;

  outputs_.push_back(format("(c) For each item in the order %d", o_id));
  while (cass_iterator_next(orderLine_it)) {
    outputs_.push_back(format(
        "\tOL_I_ID: %s, OL_SUPPLY_W_ID: %s, OL_QUANTITY: %s, OL_AMOUNT: %s, "
        "OL_DELIVER_D: %s",
        GetStringValue(GetValueFromCassRow<int32_t>(orderLine_it, "ol_i_id"))
            .c_str(),
        GetStringValue(
            GetValueFromCassRow<int32_t>(orderLine_it, "ol_supply_w_id"))
            .c_str(),
        GetStringValue(
            GetValueFromCassRow<int32_t>(orderLine_it, "ol_quantity"), 100)
            .c_str(),
        GetStringValue(GetValueFromCassRow<int64_t>(orderLine_it, "ol_amount"))
            .c_str(),
        GetStringValue(
            GetValueFromCassRow<int64_t>(orderLine_it, "ol_delivery_d"))
            .c_str()));
  }

  if (customer_it) cass_iterator_free(customer_it);
  if (order_it) cass_iterator_free(order_it);
  if (orderLine_it) cass_iterator_free(orderLine_it);
  return st;
}

std::pair<Status, CassIterator*>
YCQLOrderStatusTxn::getCustomerInfo() noexcept {
  std::string stmt =
      "SELECT c_first, c_middle, c_last, c_balance "
      "FROM " +
      YCQLKeyspace +
      ".customer "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st =
      ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_, c_d_id_, c_id_);
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Customer not found"), it};
  }
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLOrderStatusTxn::getLastOrder() noexcept {
  std::string stmt =
      "SELECT o_id, o_entry_d, o_carrier_id "
      "FROM " +
      YCQLKeyspace +
      ".orders "
      "WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? "
      "ORDER BY o_d_id DESC, o_id DESC "
      "LIMIT 1 "
      ";";
  CassIterator* it = nullptr;
  auto st =
      ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_, c_d_id_, c_id_);
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Last Order not found"), it};
  }
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLOrderStatusTxn::getOrderLines(
    int32_t o_id) noexcept {
  std::string stmt =
      "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
      "FROM " +
      YCQLKeyspace +
      ".orderline "
      "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st =
      ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_, c_d_id_, o_id);
  return {st, it};
}

}  // namespace ycql_impl
