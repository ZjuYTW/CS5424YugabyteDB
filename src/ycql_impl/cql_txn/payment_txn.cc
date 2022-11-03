#include "ycql_impl/cql_txn/payment_txn.h"

#include "ycql_impl/cql_exe_util.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLPaymentTxn::Execute(double* diff_t) noexcept {
  if (YDB_SKIP_PAYMENT) {
    *diff_t = 0;
    return Status::OK();
  }
#ifndef NDEBUG
  if (trace_timer_) {
    trace_timer_->Reset();
  }
#endif
  LOG_INFO << "Payment Transaction started";
  const auto InputString =
      format("P %d %d %d %.2f", w_id_, d_id_, c_id_, payment_);
  outputs_.reserve(10);
  auto start_time = std::chrono::system_clock::now();
  auto st =
      Retry(std::bind(&YCQLPaymentTxn::executeLocal, this), MAX_RETRY_ATTEMPTS);
  auto end_time = std::chrono::system_clock::now();
  *diff_t = (end_time - start_time).count();
  if (st.ok()) {
    LOG_INFO << "Payment Transaction completed, time cost " << *diff_t;
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

Status YCQLPaymentTxn::executeLocal() noexcept {
  Status st = Status::OK();
  CassIterator *customer_it = nullptr, *warehouse_it = nullptr,
               *district_it = nullptr;

  LOG_DEBUG << "Update Warehouse";
  st = updateWarehouseYTD();
  if (!st.ok()) return st;
  LOG_DEBUG << "Update District";
  st = updateDistrictYTD();
  if (!st.ok()) return st;
  LOG_DEBUG << "Update Customer";
  st = updateCustomerPayment();
  if (!st.ok()) return st;

  std::tie(st, customer_it) = getCustomer();
  if (!st.ok()) return st;

  outputs_.emplace_back("(a) Customer information:");
  outputs_.push_back(format("\tIdentifier: (C_W_ID,C_D_ID,C_ID)=(%d, %d, %d)",
                            w_id_, d_id_, c_id_));
  outputs_.push_back(format(
      "\tName: (C_FIRST,C_MIDDLE,C_LAST)=(%s, %s, %s)",
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_first"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_middle"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_last"))
          .c_str()));
  outputs_.push_back(format(
      "\tAddress: (C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP)=(%s, %s, %s, "
      "%s, %s)",
      GetStringValue(
          GetValueFromCassRow<std::string>(customer_it, "c_street_1"))
          .c_str(),
      GetStringValue(
          GetValueFromCassRow<std::string>(customer_it, "c_street_2"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_city"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_state"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_zip"))
          .c_str()));
  outputs_.push_back(format(
      "\tOther info: C_PHONE=%s, C_SINCE=%s, C_CREDIT=%s, C_CREDIT_LIM=%s, "
      "C_DISCOUNT=%s, C_BALANCE=%s",
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_phone"))
          .c_str(),
      GetTimeFromTS(
          GetValueFromCassRow<int64_t>(customer_it, "c_since").value())
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(customer_it, "c_credit"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<int64_t>(customer_it, "c_credit_lim"),
                     100)
          .c_str(),
      GetStringValue(GetValueFromCassRow<int64_t>(customer_it, "c_discount"),
                     10000)
          .c_str(),
      GetStringValue(GetValueFromCassRow<int64_t>(customer_it, "c_balance"),
                     100)
          .c_str()));

  std::tie(st, warehouse_it) = getWarehouse();
  if (!st.ok()) return st;

  outputs_.emplace_back("(b) Warehouse information:");
  outputs_.push_back(format(
      "\tW_STREET_1=%s, W_STREET_2=%s, W_CITY=%s, W_STATE=%s, W_ZIP=%s",
      GetStringValue(
          GetValueFromCassRow<std::string>(warehouse_it, "w_street_1"))
          .c_str(),
      GetStringValue(
          GetValueFromCassRow<std::string>(warehouse_it, "w_street_2"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(warehouse_it, "w_city"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(warehouse_it, "w_state"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(warehouse_it, "w_zip"))
          .c_str()));

  std::tie(st, district_it) = getDistrict();
  if (!st.ok()) return st;

  outputs_.emplace_back("(c) District information:");
  outputs_.push_back(format(
      "\tD_STREET_1=%s, D_STREET_2=%s, D_CITY=%s, D_STATE=%s, D_ZIP=%s",
      GetStringValue(
          GetValueFromCassRow<std::string>(district_it, "d_street_1"))
          .c_str(),
      GetStringValue(
          GetValueFromCassRow<std::string>(district_it, "d_street_2"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(district_it, "d_city"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(district_it, "d_state"))
          .c_str(),
      GetStringValue(GetValueFromCassRow<std::string>(district_it, "d_zip"))
          .c_str()));

  outputs_.push_back(format("(d) Payment: %.2f", payment_));

  if (customer_it) cass_iterator_free(customer_it);
  if (warehouse_it) cass_iterator_free(warehouse_it);
  if (district_it) cass_iterator_free(district_it);
  return st;
}

Status YCQLPaymentTxn::updateWarehouseYTD() noexcept {
  auto payment = static_cast<int64_t>(payment_ * 100);
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".warehouse "
                     "SET w_ytd = w_ytd + " +
                     std::to_string(payment) + " WHERE w_id = ? ";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_);
}

Status YCQLPaymentTxn::updateDistrictYTD() noexcept {
  auto payment = static_cast<int64_t>(payment_ * 100);
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".district "
                     "SET d_ytd = d_ytd + " +
                     std::to_string(payment) +
                     " WHERE d_w_id = ? AND d_id = ? ";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id_);
}

Status YCQLPaymentTxn::updateCustomerPayment() noexcept {
  auto payment = static_cast<int64_t>(payment_ * 100);
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".customer "
                     "SET c_balance = c_balance - " +
                     std::to_string(payment) +
                     ", c_ytd_payment = c_ytd_payment + " +
                     std::to_string(payment) +
                     ", c_payment_cnt = c_payment_cnt + 1 "
                     "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? ";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id_, c_id_);
}

Status YCQLPaymentTxn::batchUpdateCustomerDistrictWarehouse() noexcept {
  std::vector<CassStatement*> cass_stmts;
  cass_stmts.reserve(3);
  CassError rc;
  auto payment = static_cast<int64_t>(payment_ * 100);
  std::string stmt_w = "UPDATE " + YCQLKeyspace +
                       ".warehouse "
                       "SET w_ytd = w_ytd + " +
                       std::to_string(payment) + " WHERE w_id = ? ";
  auto cass_stmt_w = cass_statement_new(stmt_w.c_str(), 1);
  rc = ycql_impl::cql_statement_fill_args(cass_stmt_w, w_id_);
  assert(rc == CASS_OK);
  cass_stmts.push_back(cass_stmt_w);
  std::string stmt_d = "UPDATE " + YCQLKeyspace +
                       ".district "
                       "SET d_ytd = d_ytd + " +
                       std::to_string(payment) +
                       " WHERE d_w_id = ? AND d_id = ? ";
  auto cass_stmt_d = cass_statement_new(stmt_d.c_str(), 2);
  rc = ycql_impl::cql_statement_fill_args(cass_stmt_d, w_id_, d_id_);
  assert(rc == CASS_OK);
  cass_stmts.push_back(cass_stmt_d);
  std::string stmt_c = "UPDATE " + YCQLKeyspace +
                       ".customer "
                       "SET c_balance = c_balance - " +
                       std::to_string(payment) +
                       ", c_ytd_payment = c_ytd_payment + " +
                       std::to_string(payment) +
                       ", c_payment_cnt = c_payment_cnt + 1 "
                       "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? ";
  auto cass_stmt_c = cass_statement_new(stmt_c.c_str(), 3);
  rc = ycql_impl::cql_statement_fill_args(cass_stmt_c, w_id_, d_id_, c_id_);
  assert(rc == CASS_OK);
  cass_stmts.push_back(cass_stmt_c);
  return ycql_impl::BatchExecute(cass_stmts, conn_);
}

std::pair<Status, CassIterator*> YCQLPaymentTxn::getCustomer() noexcept {
  std::string stmt =
      "SELECT c_first, c_middle, c_last, "
      "c_street_1, c_street_2, c_city, c_state, c_zip, "
      "c_phone, c_since, c_credit, "
      "c_credit_lim, c_discount, c_balance "
      "FROM " +
      YCQLKeyspace +
      ".customer "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_, c_id_);
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLPaymentTxn::getWarehouse() noexcept {
  std::string stmt =
      "SELECT w_street_1, w_street_2, w_city, w_state, w_zip "
      "FROM " +
      YCQLKeyspace +
      ".warehouse "
      "WHERE w_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_);
  if (!st.ok()) {
    assert(!it);
    return {st, it};
  }
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLPaymentTxn::getDistrict() noexcept {
  std::string stmt =
      "SELECT d_street_1, d_street_2, d_city, d_state, d_zip "
      "FROM " +
      YCQLKeyspace +
      ".district "
      "WHERE d_w_id = ? AND d_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_);
  if (!st.ok()) {
    assert(!it);
    return {st, it};
  }
  cass_iterator_next(it);
  return {st, it};
}

}  // namespace ycql_impl