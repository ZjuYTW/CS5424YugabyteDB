#include "ycql_impl/cql_txn/order_status_txn.h"
#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;

Status YCQLOrderStatusTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Order-status Transaction started";
  int retry_time = 0;
  Status st;
  bool done = false;
  CassIterator *customer_it = nullptr,
               *order_it = nullptr,
               *orderLine_it = nullptr;
  do {
    customer_it = getCustomerInfo();
    // TODO(winston.yan): check type of c_balance
    std::cout << ydb_util::format("\t(a).C_NAME: (%s, %s, %s), C_BALANCE: %lf",
                                  GetValueFromCassRow<std::string>(customer_it, "c_first").c_str(),
                                  GetValueFromCassRow<std::string>(customer_it, "c_middle").c_str(),
                                  GetValueFromCassRow<std::string>(customer_it, "c_last").c_str(),
                                  GetValueFromCassRow<std::uint64_t>(customer_it, "c_balance") / 100.0) << std::endl;

    order_it = getLastOrder();
    auto o_id = GetValueFromCassRow<uint32_t>(order_it, "o_id");
    // TODO(winston.yan): check type of o_entry_d
    std::cout << ydb_util::format("\t(b).O_ID: %d, O_ENTRY_D: %lld, O_CARRIER_ID: %d",
                                  o_id,
                                  GetValueFromCassRow<uint64_t>(order_it, "o_entry_d"),
                                  GetValueFromCassRow<uint32_t>(order_it, "o_carrier_id")) << std::endl;

    if (!orderLine_it) {
      std::tie(st, orderLine_it) = getOrderLines(o_id);
      if (!st.ok()) continue;
    }
    std::cout << ydb_util::format("\t(c).For each item in the order %d", o_id) << std::endl;
    while (cass_iterator_next(orderLine_it)) {
      std::cout << ydb_util::format("\t\tOL_I_ID: %d, OL_SUPPLY_W_ID: %d, OL_QUANTITY: %lf, OL_AMOUNT: %d, OL_DELIVER_D: %lld",
                                    GetValueFromCassRow<uint32_t>(orderLine_it, "ol_i_id"),
                                    GetValueFromCassRow<uint32_t>(orderLine_it, "ol_supply_w_id"),
                                    GetValueFromCassRow<uint64_t>(orderLine_it, "ol_quantity") / 100.0,
                                    GetValueFromCassRow<uint32_t>(orderLine_it, "ol_amount"),
                                    GetValueFromCassRow<uint64_t>(orderLine_it, "ol_delivery_d")) << std::endl;
    }

  } while (retry_time++ < MaxRetryTime /* && sleep(done) */);
  if (customer_it) cass_iterator_free(customer_it);
  if (order_it) cass_iterator_free(order_it);
  if (orderLine_it) cass_iterator_free(orderLine_it);
  return st;
}

CassIterator *YCQLOrderStatusTxn::getCustomerInfo() {
  std::string stmt =
      "SELECT c_first, c_middle, c_last, c_balance "
      "FROM customer "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  // TODO(winston.yan): handle error
  auto _ = ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_, c_d_id_, c_id_);
  return it;
}

CassIterator *YCQLOrderStatusTxn::getLastOrder() {
  std::string stmt =
      "SELECT o_id, o_entry_d, o_carrier_id "
      "FROM orders "
      "WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? "
      "ORDER BY o_id DESC "
      "LIMIT 1 "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  // TODO(winston.yan): handle error
  auto _ = ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_, c_d_id_, c_id_);
  return it;
}

std::pair<Status, CassIterator*> YCQLOrderStatusTxn::getOrderLines(uint32_t o_id) {
  std::string stmt =
      "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
      "FROM orderline "
      "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  // TODO(winston.yan): handle error
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_, c_d_id_, o_id);
  return {st, it};
}

}
