#include "ycql_impl/cql_exe_util.h"

#include <ctime>
#include <thread>

#include "cassandra.h"

namespace ycql_impl {
bool ValidOrSleep(bool done) noexcept {
  if (!done) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return done;
}

ydb_util::Status Retry(const std::function<ydb_util::Status()>& func,
                       size_t max_attempts) {
  ydb_util::Status st;
  int attempt = 0;
  do {
    st = func();
    if (st.ok()) return st;
    ycql_impl::ValidOrSleep(false);
  } while (attempt++ < max_attempts);
  return st;
}

double GetDTax(CassIterator* district_it) noexcept {
  auto d_tax = ycql_impl::GetValueFromCassRow<int32_t>(district_it, "d_tax");
  double d_tax_d = static_cast<double>(d_tax.value()) / 10000;
  return d_tax_d;
}

double GetWTax(CassIterator* warehouse_it) noexcept {
  auto w_tax = ycql_impl::GetValueFromCassRow<int32_t>(warehouse_it, "w_tax");
  double w_tax_d = static_cast<double>(w_tax.value()) / 10000;
  return w_tax_d;
}

double GetDiscount(CassIterator* custom_it) noexcept {
  auto c_discount =
      ycql_impl::GetValueFromCassRow<int64_t>(custom_it, "c_discount");
  double c_discount_d = static_cast<double>(c_discount.value()) / 10000;
  return c_discount_d;
}

double GetPrice(CassIterator* item_it) noexcept {
  auto i_price = ycql_impl::GetValueFromCassRow<int32_t>(item_it, "i_price");
  double i_price_d = static_cast<double>(i_price.value() / 100);
  return i_price_d;
}

std::string GetTimeFromTS(int64_t longDate) noexcept {
  char buff[128];

  std::chrono::duration<int64_t, std::milli> dur(longDate);
  auto tp = std::chrono::system_clock::time_point(
      std::chrono::duration_cast<std::chrono::system_clock::duration>(dur));
  std::time_t in_time_t = std::chrono::system_clock::to_time_t(tp);
  strftime(buff, 128, "%Y-%m-%d %H:%M:%S", gmtime(&in_time_t));
  std::string resDate(buff);

  return resDate;
}

ydb_util::Status BatchExecute(const std::vector<CassStatement*>& stmts,
                              CassSession* conn) noexcept {
  auto* batch = cass_batch_new(CassBatchType::CASS_BATCH_TYPE_UNLOGGED);
  for (auto stmt : stmts) {
    cass_batch_add_statement(batch, stmt);
  }
  auto future = cass_session_execute_batch(conn, batch);
  auto rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    const char* buf = nullptr;
    size_t size;
    cass_future_error_message(future, &buf, &size);
    LOG_FATAL << "Batch execution failed, " << buf;
    return ydb_util::Status::ExecutionFailed(std::string(buf, size));
  }
  cass_batch_free(batch);
  cass_future_free(future);
  for (auto stmt : stmts) {
    cass_statement_free(stmt);
  }
  return ydb_util::Status::OK();
}

}  // namespace ycql_impl
