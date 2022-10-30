#include "ycql_impl/cql_txn/popular_item_txn.h"

#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLPopularItemTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Popular-item Transaction started";
  auto st = Retry(std::bind(&YCQLPopularItemTxn::executeLocal, this),
                  MAX_RETRY_ATTEMPTS);
  if (st.ok()) LOG_INFO << "Popular-item Transaction completed";
  return st;
}

Status YCQLPopularItemTxn::executeLocal() noexcept {
  Status st = Status::OK();

  CassIterator* next_order_it = nullptr;
  std::tie(st, next_order_it) = getNextOrder();
  if (!st.ok()) return st;
  auto next_o_id =
      GetValueFromCassRow<int32_t>(next_order_it, "d_next_o_id").value();
  if (next_order_it) cass_iterator_free(next_order_it);

  CassIterator* order_it = nullptr;
  std::tie(st, order_it) = getLastOrders(next_o_id);
  if (!st.ok()) return st;

  std::unordered_map<std::string, int> popularItems;
  int order_size = 0;
  while (cass_iterator_next(order_it)) {
    auto o_id = GetValueFromCassRow<int32_t>(order_it, "o_id").value();
    auto c_id = GetValueFromCassRow<int32_t>(order_it, "o_c_id").value();
    // TODO(winston.yan): check type of o_entry_id
    auto o_entry_d =
        GetValueFromCassRow<int64_t>(order_it, "o_entry_d").value();
    std::cout << format("\t1.Order number:(%d) & entry date and time (%lld)",
                        o_id, o_entry_d)
              << std::endl;

    CassIterator* customer_it = nullptr;
    std::tie(st, customer_it) = getCustomerName(c_id);
    if (!st.ok()) return st;
    auto c_fst =
        GetValueFromCassRow<std::string>(customer_it, "c_first").value();
    auto c_mid =
        GetValueFromCassRow<std::string>(customer_it, "c_middle").value();
    auto c_lst =
        GetValueFromCassRow<std::string>(customer_it, "c_last").value();
    if (customer_it) cass_iterator_free(customer_it);
    std::cout << format(
                     "\t2.Name of customer who placed this order (%s, %s, %s)",
                     c_fst.c_str(), c_mid.c_str(), c_lst.c_str())
              << std::endl;

    CassIterator* orderLine_it = nullptr;
    std::tie(st, orderLine_it) = getMaxOrderLines(o_id);
    if (!st.ok()) return st;
    std::cout << format("\t3.For each popular item in order %d:\n", o_id)
              << std::endl;
    // Note: Here we expect iterate just once
    while (cass_iterator_next(orderLine_it)) {
      auto ol_quantity =
          GetValueFromCassRow<int32_t>(orderLine_it, "max_quantity").value();
      auto i_ids =
          GetValueFromCassRow<std::vector<int32_t>>(orderLine_it, "item_ids")
              .value();
      for (auto i_id : i_ids) {
        CassIterator* item_it;
        std::tie(st, item_it) = getItemName(i_id);
        if (!st.ok()) return st;
        auto i_name =
            GetValueFromCassRow<std::string>(item_it, "i_name").value();
        cass_iterator_free(item_it);
        popularItems[i_name] += 1;
        std::cout << format("\t\t(i).Item name: %s", i_name.c_str())
                  << std::endl;
        std::cout << format("\t\t(i).Quantity ordered: %d", ol_quantity)
                  << std::endl;
      }
    }
    if (orderLine_it) cass_iterator_free(orderLine_it);

    ++order_size;
  }

  // print the percentage of examined orders that contain each popular item
  std::cout << "\t4.For each distinct popular item:" << std::endl;
  for (const auto& [i_name, cnt] : popularItems) {
    auto percentage = 1.0 * cnt / order_size;
    std::cout << format("\t\t(i).Item name: %s", i_name.c_str()) << std::endl;
    std::cout << format("\t\t(ii).Percentage in orders: %lf", percentage)
              << std::endl;
  }

  if (order_it) cass_iterator_free(order_it);
  return st;
}

std::pair<Status, CassIterator*> YCQLPopularItemTxn::getMaxOrderLines(
    int32_t o_id) noexcept {
  std::string stmt =
      "SELECT max_quantity, item_ids FROM " + YCQLKeyspace +
      ".order_max_quantity WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?";

  // std::string stmt =
  // "SELECT ol_i_id, MAX(ol_quantity) as max_ol_quantity "
  // "FROM " +
  // YCQLKeyspace +
  // ".orderline "
  // "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? "
  // "GROUP BY ol_i_id";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_, o_id);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLPopularItemTxn::getNextOrder() noexcept {
  std::string stmt =
      "SELECT d_next_o_id "
      "FROM " +
      YCQLKeyspace +
      ".district "
      "WHERE d_w_id = ? AND d_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_);
  if (!st.ok()) {
    return {st, it};
  }
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("District not found"), it};
  }
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLPopularItemTxn::getLastOrders(
    int32_t next_o_id) noexcept {
  std::string stmt =
      "SELECT o_id, o_c_id, o_entry_d "
      "FROM " +
      YCQLKeyspace +
      ".orders "
      "WHERE o_w_id = ? AND o_d_id = ? "
      "AND o_id >= ? AND o_id < ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_,
                                        next_o_id - l_, next_o_id);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLPopularItemTxn::getCustomerName(
    int32_t c_id) noexcept {
  std::string stmt =
      "SELECT c_first, c_middle, c_last "
      "FROM " +
      YCQLKeyspace +
      ".customer "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_, c_id);
  if (!st.ok()) {
    return {st, it};
  }
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Customer not found"), it};
  }
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLPopularItemTxn::getItemName(
    int32_t i_id) noexcept {
  std::string stmt =
      "SELECT i_name "
      "FROM " +
      YCQLKeyspace +
      ".item "
      "WHERE i_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, i_id);
  if (!st.ok()) {
    return {st, it};
  }
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Item not found"), it};
  }
  return {st, it};
}

}  // namespace ycql_impl