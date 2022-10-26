#include "ycql_impl/cql_txn/order_status_txn.h"

#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLOrderStatusTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Order-status Transaction started";
  auto st = Retry(std::bind(&YCQLOrderStatusTxn::executeLocal, this),
                  MAX_RETRY_ATTEMPTS);
  if (st.ok()) LOG_INFO << "Order-status Transaction completed";
  return st;
}

Status YCQLOrderStatusTxn::executeLocal() noexcept {
  Status st = Status::OK();
  CassIterator *customer_it = nullptr, *order_it = nullptr,
               *orderLine_it = nullptr;

  std::tie(st, customer_it) = getCustomerInfo();
  if (!st.ok()) return st;

  // TODO(winston.yan): check type of c_balance
  std::cout
      << format(
             "\t1.C_NAME: (%s, %s, %s), C_BALANCE: %lf",
             GetValueFromCassRow<std::string>(customer_it, "c_first").c_str(),
             GetValueFromCassRow<std::string>(customer_it, "c_middle").c_str(),
             GetValueFromCassRow<std::string>(customer_it, "c_last").c_str(),
             GetValueFromCassRow<std::int64_t>(customer_it, "c_balance") /
                 100.0)
      << std::endl;

  std::tie(st, order_it) = getLastOrder();
  if (!st.ok()) return st;

  auto o_id = GetValueFromCassRow<int32_t>(order_it, "o_id");
  // TODO(winston.yan): check type of o_entry_d
  std::cout << format("\t2.O_ID: %d, O_ENTRY_D: %lld, O_CARRIER_ID: %d", o_id,
                      GetValueFromCassRow<int64_t>(order_it, "o_entry_d"),
                      GetValueFromCassRow<int32_t>(order_it, "o_carrier_id"))
            << std::endl;

  std::tie(st, orderLine_it) = getOrderLines(o_id);
  if (!st.ok()) return st;

  std::cout << format("\t3.For each item in the order %d", o_id) << std::endl;
  while (cass_iterator_next(orderLine_it)) {
    std::cout
        << format(
               "\t\tOL_I_ID: %d, OL_SUPPLY_W_ID: %d, OL_QUANTITY: %lf, "
               "OL_AMOUNT: %d, OL_DELIVER_D: %lld",
               GetValueFromCassRow<int32_t>(orderLine_it, "ol_i_id"),
               GetValueFromCassRow<int32_t>(orderLine_it, "ol_supply_w_id"),
               GetValueFromCassRow<int64_t>(orderLine_it, "ol_quantity") /
                   100.0,
               GetValueFromCassRow<int32_t>(orderLine_it, "ol_amount"),
               GetValueFromCassRow<int64_t>(orderLine_it, "ol_delivery_d"))
        << std::endl;
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
      "ORDER BY o_id DESC "
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
