#include "ycql_impl/cql_txn/delivery_txn.h"

#include <cassert>
#include <future>

#include "cassandra.h"
#include "common/util/string_util.h"
#include "ycql_impl/cql_exe_util.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLDeliveryTxn::Execute(double* diff_t) noexcept {
  if (YDB_SKIP_DELIVERY) {
    *diff_t = 0;
    return Status::OK();
  }
#ifndef NDEBUG
  if (trace_timer_) {
    trace_timer_->Reset();
  }
#endif
  LOG_INFO << "Delivery transaction started";
  const auto InputString = format("D %d %d", w_id_, carrier_id_);
  Status st = Status::OK();
  auto DeliveryInput = ydb_util::format("D %d %d", w_id_, carrier_id_);

  auto start = std::chrono::system_clock::now();
  std::vector<std::future<Status>> fts;
  for (int32_t d_id = 1; d_id <= 10; ++d_id) {
    LOG_INFO << "Delivery process on d_id[" << d_id << "]";
    auto exec_one_district = [this, d_id](int32_t i) {
      return Retry(std::bind(&YCQLDeliveryTxn::executeLocal, this, d_id),
                   MAX_RETRY_ATTEMPTS);
    };
    fts.push_back(std::async(std::launch::async, exec_one_district, d_id));
  }
  int cnt = 0;
  for (auto& ft : fts) {
    st = ft.get();
    if (!st.ok()) {
      LOG_FATAL << "Delivery transaction distrcit [ " << ++cnt
                << "] execution failed, " << st.ToString();
      continue;
    }
  }
  auto end_time = std::chrono::system_clock::now();
  *diff_t = (end_time - start).count();
  if (st.ok()) {
    LOG_INFO << "Delivery transaction completed, time cost: " << *diff_t;
    txn_out_ << InputString << std::endl;
  } else {
    err_out_ << InputString << std::endl;
    err_out_ << st.ToString() << std::endl;
  }
#ifndef NDEBUG
  if (trace_timer_) trace_timer_->Print();
#endif
  return (st.ok() || st.isEndOfFile()) ? Status::OK() : st;
}

Status YCQLDeliveryTxn::executeLocal(int32_t d_id) noexcept {
  Status st = Status::OK();

  CassIterator *order_it = nullptr, *amount_it = nullptr;
  const CassResult *order_result = nullptr, *amount_result = nullptr;
  LOG_INFO << "Get Next Delivery Order";
  auto free_func = [&order_it, &amount_it, &order_result, &amount_result]() {
    if (order_it) cass_iterator_free(order_it);
    if (amount_it) cass_iterator_free(amount_it);
    if (order_result) cass_result_free(order_result);
    if (amount_result) cass_result_free(amount_result);
  };
  DEFER(std::move(free_func));

  std::tie(st, order_it) = getNextDeliveryOrder(d_id, &order_result);
  if (!st.ok()) {
    // if can't find a null delivery order, just return ok
    if (st.isEndOfFile()) {
      return Status::OK();
    }
    return st;
  }
  auto o_id = GetValueFromCassRow<int32_t>(order_it, "o_id").value();
  auto c_id = GetValueFromCassRow<int32_t>(order_it, "o_c_id").value();
  LOG_INFO << "Delete Next Delivery Order";
  st = deleteNextDeliveryOrder(d_id, o_id);
  if (!st.ok()) {
    return st;
  }

  LOG_DEBUG << "Update Carrier Id";
  bool suc;
  std::tie(st, suc) = updateCarrierId(o_id, d_id);
  if (!st.ok() || !suc) return st;

  LOG_DEBUG << "Update Order Line Delivery Date";
  st = updateOrderLineDeliveryDate(o_id, d_id);
  if (!st.ok()) return st;

  LOG_DEBUG << "Get OrderPaymentAmount";
  std::tie(st, amount_it) = getOrderPaymentAmount(o_id, d_id, &amount_result);
  if (!st.ok()) return st;
  auto total_amount =
      GetValueFromCassRow<int64_t>(amount_it, "sum_ol_amount").value();

  LOG_DEBUG << "update Customer Bal And Delivery Cnt";
  st = updateCustomerBalAndDeliveryCnt(c_id, total_amount, d_id);
  if (!st.ok()) {
    LOG_DEBUG << "update Customer Bal failed, " << st.ToString();
    return st;
  }
  LOG_DEBUG << "Success";
  return st;
}

Status YCQLDeliveryTxn::deleteNextDeliveryOrder(int32_t d_id,
                                                int32_t o_id) noexcept {
  if (d_id == 1) {
    TRACE_GUARD
  }
  std::string stmt =
      "DELETE FROM " + YCQLKeyspace +
      ".order_non_delivery WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id, o_id);
}

std::pair<Status, CassIterator*> YCQLDeliveryTxn::getNextDeliveryOrder(
    int32_t d_id, const CassResult** result) noexcept {
  // Note: we just record one portion
  if (d_id == 1) {
    TRACE_GUARD
  }
  std::string stmt =
      "SELECT o_id, o_c_id "
      "FROM " +
      YCQLKeyspace +
      ".order_non_delivery "
      "WHERE o_w_id = ? AND o_d_id = ? "
      "ORDER BY o_d_id ASC, o_id ASC "
      "LIMIT 1 ";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id_, d_id);
  if (!st.ok()) {
    assert(!it);
    LOG_DEBUG << st.ToString();
    return {st, it};
  }
  if (!cass_iterator_next(it)) {
    LOG_DEBUG << "Next delivery order on [" << w_id_ << "," << d_id
              << "] not found";
    // This means we can't find a coressponding avaliable order, just skip it
    return {Status::EndOfFile("Next Delivery Order not found"), it};
  }
  return {st, it};
}

std::pair<Status, bool> YCQLDeliveryTxn::updateCarrierId(
    int32_t o_id, int32_t d_id) noexcept {
  // Note: we just record one portion
  if (d_id == 1) {
    TRACE_GUARD
  }
  // Note: Here we need to do a CAS for avoid race condition
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".orders "
                     "SET o_carrier_id = ? "
                     "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? "
                     "IF o_carrier_id = null;";
  CassStatement* statement = cass_statement_new(stmt.c_str(), 4);
  auto rc = ycql_impl::cql_statement_fill_args(statement, carrier_id_, w_id_,
                                               d_id, o_id);
  assert(rc == CASS_OK);
  auto future = cass_session_execute(conn_, statement);
  auto result = cass_future_get_result(future);
  if (result == nullptr) {
    cass_statement_free(statement);
    cass_future_free(future);
    return {Status::ExecutionFailed("Update Carrier ID failed"), false};
  }
  cass_bool_t appiled;
  auto value = cass_row_get_column(cass_result_first_row(result), 0);
  cass_value_get_bool(value, &appiled);
  cass_statement_free(statement);
  cass_future_free(future);
  cass_result_free(result);
  return {Status::OK(), value};
}

Status YCQLDeliveryTxn::updateOrderLineDeliveryDate(int32_t o_id,
                                                    int32_t d_id) noexcept {
  // Note: we just record one portion
  if (d_id == 1) {
    TRACE_GUARD
  }
  std::vector<std::pair<int32_t, int32_t>> lines;
  {
    const CassResult* result = nullptr;
    auto [st, it] = getAllOrderLineNumber(o_id, d_id, &result);
    if (!st.ok()) {
      assert(!it);
      LOG_DEBUG << "Get All Order Line Number failed, " << st.ToString();
      return st;
    }
    while (cass_iterator_next(it)) {
      auto ol_i_id = GetValueFromCassRow<int32_t>(it, "ol_i_id").value();
      auto ol_number = GetValueFromCassRow<int32_t>(it, "ol_number").value();
      lines.emplace_back(ol_i_id, ol_number);
    }
    cass_iterator_free(it);
    cass_result_free(result);
  }

  std::vector<CassStatement*> cass_stmts;
  cass_stmts.reserve(lines.size());
  for (auto& [ol_i_id, ol_number] : lines) {
    std::string stmt = "UPDATE " + YCQLKeyspace +
                       ".orderline "
                       "SET ol_delivery_d = currenttimestamp() "
                       "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND "
                       "ol_number = ? AND ol_i_id = ?";
    auto cass_stmt = cass_statement_new(stmt.c_str(), 5);
    auto rc = ycql_impl::cql_statement_fill_args(cass_stmt, w_id_, d_id, o_id,
                                                 ol_number, ol_i_id);
    assert(rc == CASS_OK);
    cass_stmts.push_back(cass_stmt);
  }
  return ycql_impl::BatchExecute(cass_stmts, conn_);
}

std::pair<Status, CassIterator*> YCQLDeliveryTxn::getOrderPaymentAmount(
    int32_t o_id, int32_t d_id, const CassResult** result) noexcept {
  // Note: we just record one portion
  if (d_id == 1) {
    TRACE_GUARD
  }
  std::string stmt =
      "SELECT total_amount as sum_ol_amount "
      "FROM " +
      YCQLKeyspace +
      ".order_max_quantity "
      "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? ";
  CassIterator* it = nullptr;
  auto st =
      ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id_, d_id, o_id);
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Order Payment Amount not found"), it};
  }
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLDeliveryTxn::getAllOrderLineNumber(
    int32_t o_id, int32_t d_id, const CassResult** result) noexcept {
  // Note: we just record one portion
  if (d_id == 1) {
    TRACE_GUARD
  }
  std::string stmt =
      "SELECT ol_i_id, ol_number FROM " + YCQLKeyspace +
      ".orderline where ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?";
  CassIterator* it = nullptr;
  auto st =
      ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id_, d_id, o_id);
  return {st, it};
}

Status YCQLDeliveryTxn::updateCustomerBalAndDeliveryCnt(int32_t c_id,
                                                        int64_t total_amount,
                                                        int32_t d_id) noexcept {
  // Note: we just record one portion
  if (d_id == 1) {
    TRACE_GUARD
  }
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".customer "
                     "SET c_balance = c_balance + " +
                     std::to_string(total_amount) +
                     ", c_delivery_cnt = c_delivery_cnt + 1 "
                     "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
  return ycql_impl::execute_write_cql(conn_, stmt, (int32_t)w_id_, d_id, c_id);
}

};  // namespace ycql_impl