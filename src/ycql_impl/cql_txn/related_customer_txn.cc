#include "ycql_impl/cql_txn/related_customer_txn.h"

#include <thread>

#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ycql_impl::ValidOrSleep;
using ydb_util::format;

Status YCQLRelatedCustomerTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Related-customer transaction started";
  auto st = Retry(std::bind(&YCQLRelatedCustomerTxn::executeLocal, this),
                  MAX_RETRY_ATTEMPTS);
  if (st.ok()) LOG_INFO << "Related-customer transaction completed";
  return st;
}

Status YCQLRelatedCustomerTxn::executeLocal() noexcept {
  std::cout << format("\t1. Customer identifier (%d, %d, %d)", c_w_id_, c_d_id_,
                      c_id_)
            << std::endl;

  Status st = Status::OK();

  CassIterator* order_it = nullptr;
  std::tie(st, order_it) = getOrders();
  if (!st.ok()) return st;

  std::unordered_map<int32_t, std::string> customers;
  while (cass_iterator_next(order_it)) {
    auto o_id = GetValueFromCassRow<int32_t>(order_it, "o_id").value();
    LOG_INFO << "o_id: " << o_id;

    CassIterator* orderLine_it = nullptr;
    std::tie(st, orderLine_it) = getOrderLines(o_id);
    if (!st.ok()) return st;

    std::vector<int32_t> items;
    while (cass_iterator_next(orderLine_it)) {
      items.push_back(
          GetValueFromCassRow<int32_t>(orderLine_it, "ol_i_id").value());
    }

    st = addRelatedCustomers(items, customers);
    if (!st.ok()) return st;
    if (orderLine_it) cass_iterator_free(orderLine_it);
  }

  std::cout << "\t2. For each related customer:" << std::endl;
  for (const auto& [c_id, customer_info] : customers) {
    std::cout << "\t\t" << customer_info << std::endl;
  }

  if (order_it) cass_iterator_free(order_it);
  return st;
}

Status YCQLRelatedCustomerTxn::addRelatedCustomers(
    const std::vector<int32_t>& i_ids,
    std::unordered_map<int32_t, std::string>& customers) noexcept {
  Status st = Status::OK();
  if (i_ids.size() < 2) return st;

  CassIterator* order_it = nullptr;
  std::tie(st, order_it) = getRelatedOrders(i_ids);
  if (!st.ok()) return st;
  std::unordered_map<const order_key_t, int, order_key_hash> order_counter;
  while (cass_iterator_next(order_it)) {
    auto ol_w_id = GetValueFromCassRow<int32_t>(order_it, "ol_w_id").value();
    auto ol_d_id = GetValueFromCassRow<int32_t>(order_it, "ol_d_id").value();
    auto ol_o_id = GetValueFromCassRow<int32_t>(order_it, "ol_o_id").value();
    ++order_counter[std::make_tuple(ol_w_id, ol_d_id, ol_o_id)];
  }
  cass_iterator_free(order_it);

  for (const auto &[order_key, count] : order_counter) {
    LOG_INFO << "(" << std::get<0>(order_key)
        << ", " << std::get<1>(order_key)
        << ", " << std::get<2>(order_key)
        << ") -> " << count;
    if (count < THRESHOLD) continue;

    int32_t ol_w_id = std::get<0>(order_key),
            ol_d_id = std::get<1>(order_key),
            ol_o_id = std::get<2>(order_key);
    CassIterator* customer_it = nullptr;
    std::tie(st, customer_it) = getCustomerId(ol_w_id, ol_d_id, ol_o_id);
    if (!st.ok()) return st;

    auto c_id = GetValueFromCassRow<int32_t>(customer_it, "o_c_id").value();
    if (customers.find(c_id) != customers.end()) continue;
    customers[c_id] = format("(%d, %d, %d)", ol_w_id, ol_d_id, c_id);

    if (customer_it) cass_iterator_free(customer_it);
  }
  return st;
}

std::pair<Status, CassIterator*> YCQLRelatedCustomerTxn::getRelatedOrders(
    const std::vector<int32_t>& i_ids) noexcept {
  std::string stmt =
      "SELECT ol_w_id, ol_d_id, ol_o_id "
      "FROM " +
      YCQLKeyspace +
      ".orderline "
      "WHERE ol_w_id != ? "
      "AND ol_i_id IN (" +
      idSetToString(i_ids) +
      ") "
      ";";
  CassIterator* it = nullptr;
  LOG_INFO << stmt;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLRelatedCustomerTxn::getCustomerId(
    int32_t w_id, int32_t d_id, int32_t o_id) noexcept {
  std::string stmt =
      "SELECT o_c_id "
      "FROM " +
      YCQLKeyspace +
      ".orders "
      "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id, d_id, o_id);
  LOG_INFO << stmt;
  if (!st.ok()) return {st, it};
  if (!cass_iterator_next(it))
    return {Status::ExecutionFailed("Customer not found"), it};
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLRelatedCustomerTxn::getOrders() noexcept {
  std::string stmt =
      "SELECT o_id "
      "FROM " +
      YCQLKeyspace +
      ".orders "
      "WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? "
      ";";
  CassIterator* it = nullptr;
  LOG_INFO << stmt;
  auto st =
      ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_, c_d_id_, c_id_);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLRelatedCustomerTxn::getOrderLines(
    int32_t o_id) noexcept {
  std::string stmt =
      "SELECT ol_i_id "
      "FROM " +
      YCQLKeyspace +
      ".orderline "
      "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? "
      ";";
  CassIterator* it = nullptr;
  LOG_INFO << stmt;
  auto st =
      ycql_impl::execute_read_cql(conn_, stmt, &it, c_w_id_, c_d_id_, o_id);
  return {st, it};
}

std::string YCQLRelatedCustomerTxn::idSetToString(const std::vector<int32_t>& i_ids) noexcept {
  auto n = i_ids.size();
  assert(n >= 2);
  std::string ret = std::to_string(i_ids[0]);
  for (int i = 1; i < n; ++i) ret += "," + std::to_string(i_ids[i]);
  return ret;
}


std::size_t YCQLRelatedCustomerTxn::order_key_hash::operator()(const ycql_impl::YCQLRelatedCustomerTxn::order_key_t& k) const {
  return std::get<0>(k) ^ std::get<1>(k) ^ std::get<2>(k);
}

}  // namespace ycql_impl