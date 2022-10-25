#include "ycql_impl/cql_exe_util.h"

#include <thread>

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
  double d_tax_d = static_cast<double>(d_tax) / 10000;
  return d_tax_d;
}

double GetWTax(CassIterator* warehouse_it) noexcept {
  auto w_tax = ycql_impl::GetValueFromCassRow<int32_t>(warehouse_it, "w_tax");
  double w_tax_d = static_cast<double>(w_tax) / 10000;
  return w_tax_d;
}

double GetDiscount(CassIterator* custom_it) noexcept {
  auto c_discount = ycql_impl::GetValueFromCassRow<int64_t>(custom_it, "c_discount");
  double c_discount_d = static_cast<double>(c_discount) / 10000;
  return c_discount_d;
}

}  // namespace ycql_impl
