#include "ycql_impl/cql_txn/delivery_txn.h"

#include <future>
#include <cassert>

#include "cassandra.h"
#include "common/util/string_util.h"
#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLDeliveryTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Delivery transaction started";
  const auto InputString = format("D %d %d", w_id_, carrier_id_);
  auto start_time = std::chrono::system_clock::now();
  Status st = Status::OK();
  auto DeliveryInput = ydb_util::format("D %d %d", w_id_, carrier_id_);

  auto start = std::chrono::system_clock::now();
  std::vector<std::future<Status>> fts;
  for (d_id_ = 1; d_id_ <= 10; ++d_id_) {
    LOG_INFO << "Delivery process on d_id[" << d_id_ << "]";
    auto exec_one_district = [this](int32_t i) {
      return Retry(std::bind(&YCQLDeliveryTxn::executeLocal, this),
                 MAX_RETRY_ATTEMPTS);
    };
    fts.push_back(std::async(std::launch::async, exec_one_district, d_id_));
  }
  for (auto &ft: fts) {
    if (!ft.get().ok()) {
      LOG_FATAL << "Delivery transaction execution failed, " << st.ToString();
      break;
    }
  }
  auto end_time = std::chrono::system_clock::now();
  *diff_t = (end_time - start_time).count();
  if (st.ok() || st.isEndOfFile()) {
    LOG_INFO << "Delivery transaction completed";
    txn_out_ << InputString << std::endl;
  } else {
    err_out_ << InputString << std::endl;
    err_out_ << st.ToString() << std::endl;
  }
  return st;
}

Status YCQLDeliveryTxn::executeLocal() noexcept {
  Status st = Status::OK();

  CassIterator* order_it = nullptr;
  LOG_DEBUG << "Get Next Delivery Order";
  std::tie(st, order_it) = getNextDeliveryOrder();
  if (!st.ok()) return st;
  auto o_id = GetValueFromCassRow<int32_t>(order_it, "o_id").value();
  auto c_id = GetValueFromCassRow<int32_t>(order_it, "o_c_id").value();
  if (order_it) cass_iterator_free(order_it);

  LOG_DEBUG << "Update Carrier Id";
  st = updateCarrierId(o_id);
  if (!st.ok()) return st;

  LOG_DEBUG << "Update Order Line Delivery Date";
  st = updateOrderLineDeliveryDate(o_id);
  if (!st.ok()) return st;

  LOG_DEBUG << "Get OrderPaymentAmount";
  CassIterator* amount_it = nullptr;
  std::tie(st, amount_it) = getOrderPaymentAmount(o_id);
  if (!st.ok()) return st;
  auto total_amount =
      GetValueFromCassRow<int64_t>(amount_it, "sum_ol_amount").value();
  if (amount_it) cass_iterator_free(amount_it);

  LOG_DEBUG << "update Customer Bal And Delivery Cnt";
  st = updateCustomerBalAndDeliveryCnt(c_id, total_amount);
  if (!st.ok()) {
    LOG_DEBUG << "update Customer Bal failed, " << st.ToString();
    return st;
  }
  LOG_DEBUG << "Success";
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
  std::vector<std::pair<int32_t, int32_t>> lines;
  {
    auto [st, it] = getAllOrderLineNumber(o_id);
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
    auto rc = ycql_impl::cql_statement_fill_args(cass_stmt, w_id_, d_id_, o_id,
                                                 ol_number, ol_i_id);
    assert(rc == CASS_OK);
    cass_stmts.push_back(cass_stmt);
  }
  return ycql_impl::BatchExecute(cass_stmts, conn_);
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

std::pair<Status, CassIterator*> YCQLDeliveryTxn::getAllOrderLineNumber(
    int32_t o_id) noexcept {
  std::string stmt =
      "SELECT ol_i_id, ol_number FROM " + YCQLKeyspace +
      ".orderline where ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_, o_id);
  return {st, it};
}

Status YCQLDeliveryTxn::updateCustomerBalAndDeliveryCnt(
    int32_t c_id, int64_t total_amount) noexcept {
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".customer "
                     "SET c_balance = c_balance + " +
                     std::to_string(total_amount) +
                     ", c_delivery_cnt = c_delivery_cnt + 1 "
                     "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
  return ycql_impl::execute_write_cql(conn_, stmt, (int32_t)w_id_, d_id_, c_id);
}

};  // namespace ycql_impl