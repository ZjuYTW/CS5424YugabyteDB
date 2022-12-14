#include "ycql_impl/cql_txn/top_balance_txn.h"

#include <future>
#include <thread>

#include "ycql_impl/cql_exe_util.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLTopBalanceTxn::Execute(double* diff_t) noexcept {
  if (YDB_SKIP_TOP_BALANCE) {
    *diff_t = 0;
    return Status::OK();
  }
#ifndef NDEBUG
  if (trace_timer_) {
    trace_timer_->Reset();
  }
#endif
  LOG_INFO << "Top-Balance Transaction started";
  outputs_.reserve(TOP_K * 4);
  auto start_time = std::chrono::system_clock::now();
  auto st = Retry(std::bind(&YCQLTopBalanceTxn::executeLocal, this),
                  MAX_RETRY_ATTEMPTS);
  auto end_time = std::chrono::system_clock::now();
  *diff_t = (end_time - start_time).count();
  if (st.ok()) {
    LOG_INFO << "Top-Balance Transaction completed, time cost " << *diff_t;
    // Txn output: reverse top balance customer output order
    auto n = outputs_.size();
    assert(n % 4 == 0);
    n /= 4;
    txn_out_ << "T" << std::endl;
    for (int i = n - 1; i >= 0; --i) {
      for (int j = i * 4; j < (i + 1) * 4; ++j) {
        txn_out_ << "\t" << outputs_[j] << std::endl;
      }
    }
  } else {
    err_out_ << "T" << std::endl;
    err_out_ << st.ToString() << std::endl;
  }
  return st;
}

Status YCQLTopBalanceTxn::executeLocal() noexcept {
  Status st = Status::OK();

  std::vector<std::future<std::vector<CustomerInfo>>> fts;
  fts.reserve(10);

  for (size_t i = 1; i <= 10; i++) {
    auto exec_one_distrcit = [this](size_t i) {
      while (true) {
        Status st;
        CassIterator* customer_it = nullptr;
        const CassResult* customer_result = nullptr;
        std::tie(st, customer_it) = this->getCustomers(i, &customer_result);
        auto free_func = [&customer_it, &customer_result]() {
          if (customer_it) cass_iterator_free(customer_it);
          if (customer_result) cass_result_free(customer_result);
        };
        DEFER(std::move(free_func));

        if (!st.ok()) continue;
        std::priority_queue<CustomerInfo, std::vector<CustomerInfo>,
                            std::greater<>>
            top_customers;
        while (cass_iterator_next(customer_it)) {
          auto c_bal =
              GetValueFromCassRow<int64_t>(customer_it, "c_balance").value();
          if (top_customers.size() == TOP_K &&
              top_customers.top().c_bal >= c_bal)
            continue;
          auto w_id =
              GetValueFromCassRow<int32_t>(customer_it, "c_w_id").value();
          auto d_id =
              GetValueFromCassRow<int32_t>(customer_it, "c_d_id").value();
          auto c_id = GetValueFromCassRow<int32_t>(customer_it, "c_id").value();
          top_customers.push(CustomerInfo{
              .c_bal = c_bal, .c_w_id = w_id, .c_d_id = d_id, .c_id = c_id});
          if (top_customers.size() > TOP_K) top_customers.pop();
          assert(top_customers.size() <= 10);
        }
        std::vector<CustomerInfo> res;
        while (!top_customers.empty()) {
          res.push_back(std::move(top_customers.top()));
          top_customers.pop();
        }
        return res;
      }
    };
    fts.push_back(std::async(std::launch::async, exec_one_distrcit, i));
  }
  std::priority_queue<CustomerInfo, std::vector<CustomerInfo>, std::greater<>>
      pq;
  for (size_t i = 1; i <= 10; i++) {
    auto part_customers = fts[i - 1].get();
    for (auto& part_cstomer : part_customers) {
      pq.push(std::move(part_cstomer));
    }
  }

  while (!pq.empty()) {
    auto customer = pq.top();
    CassIterator *customer_it = nullptr, *warehouse_it = nullptr,
                 *district_it = nullptr;
    const CassResult *customer_result = nullptr, *warehouse_result = nullptr,
                     *district_result = nullptr;
    auto free_func = [&customer_it, &warehouse_it, &district_it,
                      &customer_result, &warehouse_result, &district_result]() {
      if (customer_it) cass_iterator_free(customer_it);
      if (warehouse_it) cass_iterator_free(warehouse_it);
      if (district_it) cass_iterator_free(district_it);
      if (customer_result) cass_result_free(customer_result);
      if (warehouse_result) cass_result_free(warehouse_result);
      if (district_result) cass_result_free(district_result);
    };
    DEFER(std::move(free_func));

    std::tie(st, customer_it) = getCustomerName(customer, &customer_result);
    if (!st.ok()) return st;
    auto c_fst =
        GetValueFromCassRow<std::string>(customer_it, "c_first").value();
    auto c_mid =
        GetValueFromCassRow<std::string>(customer_it, "c_middle").value();
    auto c_lst =
        GetValueFromCassRow<std::string>(customer_it, "c_last").value();

    std::tie(st, warehouse_it) =
        getWarehouse(customer.c_w_id, &warehouse_result);
    if (!st.ok()) return st;
    auto w_name = GetValueFromCassRow<std::string>(warehouse_it, "w_name");
    std::tie(st, district_it) =
        getDistrict(customer.c_w_id, customer.c_d_id, &district_result);
    if (!st.ok()) return st;
    auto d_name = GetValueFromCassRow<std::string>(district_it, "d_name");

    outputs_.push_back(format("(a) Name of Customer: (%s, %s, %s)",
                              c_fst.c_str(), c_mid.c_str(), c_lst.c_str()));
    outputs_.push_back(format("(b) Customer's Balance: %lf",
                              static_cast<double>(customer.c_bal / 100.0)));
    outputs_.push_back(format("(c) Customer's Warehouse: %s", w_name->c_str()));
    outputs_.push_back(format("(d) Customer's District: %s", d_name->c_str()));

    pq.pop();
  }

  return st;
}

std::pair<Status, CassIterator*> YCQLTopBalanceTxn::getCustomers(
    int32_t d_id, const CassResult** result) noexcept {
  std::string stmt =
      "SELECT c_w_id, c_d_id, c_id, c_balance "
      "FROM " +
      YCQLKeyspace +
      ".customer WHERE c_d_id = ?"
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, d_id);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLTopBalanceTxn::getCustomerName(
    const CustomerInfo& c_info, const CassResult** result) noexcept {
  std::string stmt =
      "SELECT c_first, c_middle, c_last "
      "FROM " +
      YCQLKeyspace +
      ".customer "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, c_info.c_w_id,
                                        c_info.c_d_id, c_info.c_id);
  if (!st.ok()) {
    return {st, it};
  }
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Customer not found"), it};
  }
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLTopBalanceTxn::getWarehouse(
    int32_t w_id, const CassResult** result) noexcept {
  std::string stmt =
      "SELECT w_name "
      "FROM " +
      YCQLKeyspace +
      ".warehouse "
      "WHERE w_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id);
  if (!st.ok()) {
    return {st, it};
  }
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Warehouse not found"), it};
  }
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLTopBalanceTxn::getDistrict(
    int32_t w_id, int32_t d_id, const CassResult** result) noexcept {
  std::string stmt =
      "SELECT d_name "
      "FROM " +
      YCQLKeyspace +
      ".district "
      "WHERE d_w_id = ? and d_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id, d_id);
  if (!st.ok()) {
    return {st, it};
  }
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("District not found"), it};
  }
  return {st, it};
}

};  // namespace ycql_impl