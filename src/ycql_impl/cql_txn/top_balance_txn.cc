#include "ycql_impl/cql_txn/top_balance_txn.h"

#include <thread>

#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLTopBalanceTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Top-Balance Transaction started";
  auto st = Retry(std::bind(&YCQLTopBalanceTxn::executeLocal, this),
                  MAX_RETRY_ATTEMPTS);
  if (st.ok()) LOG_INFO << "Top-Balance Transaction completed";
  return st;
}

Status YCQLTopBalanceTxn::executeLocal() noexcept {
  Status st = Status::OK();
  CassIterator* customer_it = nullptr;

  std::tie(st, customer_it) = getTopBalCustomers();
  if (!st.ok()) return st;

  std::cout << "For each top-balanced customer:" << std::endl;
  int top_i = 1;

  while (cass_iterator_next(customer_it)) {
    CassIterator *warehouse_it = nullptr, *district_it = nullptr;
    auto w_id = GetValueFromCassRow<int32_t>(customer_it, "c_w_id");
    auto d_id = GetValueFromCassRow<int32_t>(customer_it, "c_d_id");
    auto c_bal = GetValueFromCassRow<int64_t>(customer_it, "c_balance");
    auto c_fst = GetValueFromCassRow<std::string>(customer_it, "c_first");
    auto c_mid = GetValueFromCassRow<std::string>(customer_it, "c_middle");
    auto c_lst = GetValueFromCassRow<std::string>(customer_it, "c_last");

    LOG_INFO << "W_ID: " << w_id << ", D_ID: " << d_id;

    std::tie(st, warehouse_it) = getWarehouse(w_id);
    if (!st.ok()) return st;
    auto w_name = GetValueFromCassRow<std::string>(warehouse_it, "w_name");
    std::tie(st, district_it) = getDistrict(w_id, d_id);
    if (!st.ok()) return st;
    auto d_name = GetValueFromCassRow<std::string>(district_it, "d_name");

    std::cout << format("\t%d. Customer name: (%s, %s, %s)", top_i++,
                        c_fst.c_str(), c_mid.c_str(), c_lst.c_str())
              << std::endl;
    std::cout << format("\t\tCustomer balance: %lf",
                        static_cast<double>(c_bal / 100.0))
              << std::endl;
    std::cout << format("\t\tWarehouse & district name: %s, %s", w_name.c_str(),
                        d_name.c_str())
              << std::endl;

    if (warehouse_it) cass_iterator_free(warehouse_it);
    if (district_it) cass_iterator_free(district_it);
  }

  if (customer_it) cass_iterator_free(customer_it);
  return st;
}

std::pair<Status, CassIterator*>
YCQLTopBalanceTxn::getTopBalCustomers() noexcept {
  std::string stmt =
      "SELECT c_w_id, c_d_id, c_balance, c_first, c_middle, c_last "
      "FROM " +
      YCQLKeyspace +
      ".customer "
      "ORDER BY c_balance DESC "
      "LIMIT ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, TOP_K);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLTopBalanceTxn::getWarehouse(
    int32_t w_id) noexcept {
  std::string stmt =
      "SELECT w_name "
      "FROM " +
      YCQLKeyspace +
      ".warehouse "
      "WHERE w_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLTopBalanceTxn::getDistrict(
    int32_t w_id, int32_t d_id) noexcept {
  std::string stmt =
      "SELECT d_name "
      "FROM " +
      YCQLKeyspace +
      ".district "
      "WHERE d_w_id = ? and d_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id, d_id);
  return {st, it};
}

};  // namespace ycql_impl