#include "ycql_impl/cql_txn/payment_txn.h"

#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLPaymentTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Payment Transaction started";
  auto st =
      Retry(std::bind(&YCQLPaymentTxn::executeLocal, this), MAX_RETRY_ATTEMPTS);
  if (st.ok()) LOG_INFO << "Payment Transaction completed";
  return st;
}

Status YCQLPaymentTxn::executeLocal() noexcept {
  Status st = Status::OK();
  CassIterator *customer_it = nullptr, *warehouse_it = nullptr,
               *district_it = nullptr;

  st = updateWarehouseYTD();
  if (!st.ok()) return st;
  st = updateDistrictYTD();
  if (!st.ok()) return st;
  st = updateCustomerPayment();
  if (!st.ok()) return st;

  std::tie(st, customer_it) = getCustomer();
  if (!st.ok()) return st;

  std::cout << "\t(a).Customer information:" << std::endl;
  std::cout << format("\t\tIdentifier: (%d, %d, %d)", w_id_, d_id_, c_id_)
            << std::endl;
  std::cout << format("\t\tName: (%s, %s, %s)",
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_first"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_middle"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_last"))
                          .c_str())
            << std::endl;
  std::cout << format("\t\tAddress: (%s, %s, %s, %s, %s)",
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_street_1"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_street_2"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_city"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_state"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_zip"))
                          .c_str())
            << std::endl;
  std::cout << format("\t\tC_PHONE: %s",
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_phone"))
                          .c_str())
            << std::endl;
  std::cout << format("\t\tC_SINCE: %s",
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_since"))
                          .c_str())
            << std::endl;
  std::cout << format("\t\tC_CREDIT: %s",
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         customer_it, "c_credit"))
                          .c_str())
            << std::endl;
  std::cout << format("\t\tC_CREDIT_LIM: %s",
                      GetStringValue(GetValueFromCassRow<double>(
                                         customer_it, "c_credit_lim"))
                          .c_str())
            << std::endl;
  std::cout << format("\t\tC_DISCOUNT: %s",
                      GetStringValue(GetValueFromCassRow<double>(customer_it,
                                                                 "c_discount"))
                          .c_str())
            << std::endl;
  std::cout << format("\t\tC_BALANCE: %s",
                      GetStringValue(GetValueFromCassRow<int64_t>(customer_it,
                                                                  "c_balance"),
                                     100)
                          .c_str())
            << std::endl;

  std::tie(st, warehouse_it) = getWarehouse();
  if (!st.ok()) return st;

  std::cout << "\t(b).Warehouse information:" << std::endl;
  std::cout << format("\t\tAddress: (%s, %s, %s, %s, %s)",
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         warehouse_it, "w_street_1"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         warehouse_it, "w_street_2"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         warehouse_it, "w_city"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         warehouse_it, "w_state"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         warehouse_it, "w_zip"))
                          .c_str())
            << std::endl;

  std::tie(st, district_it) = getDistrict();
  if (!st.ok()) return st;

  std::cout << "\t(c).District information:" << std::endl;
  std::cout << format("\t\tAddress: (%s, %s, %s, %s, %s)",
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         district_it, "d_street_1"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         district_it, "d_street_2"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         district_it, "d_city"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         district_it, "d_state"))
                          .c_str(),
                      GetStringValue(GetValueFromCassRow<std::string>(
                                         district_it, "d_zip"))
                          .c_str())
            << std::endl;

  if (customer_it) cass_iterator_free(customer_it);
  if (warehouse_it) cass_iterator_free(warehouse_it);
  if (district_it) cass_iterator_free(district_it);
  return st;
}

Status YCQLPaymentTxn::updateWarehouseYTD() noexcept {
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".warehouse "
                     "SET w_ytd = w_ytd + ? "
                     "WHERE w_id = ? "
                     ";";
  CassIterator* it = nullptr;
  auto payment = static_cast<uint64_t>(payment_ * 100);
  return ycql_impl::execute_write_cql(conn_, stmt, &it, payment, w_id_);
}

Status YCQLPaymentTxn::updateDistrictYTD() noexcept {
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".district "
                     "SET d_ytd = d_ytd + ? "
                     "WHERE w_id = ? AND d_id = ? "
                     ";";
  CassIterator* it = nullptr;
  auto payment = static_cast<uint64_t>(payment_ * 100);
  return ycql_impl::execute_write_cql(conn_, stmt, &it, payment, w_id_, d_id_);
}

Status YCQLPaymentTxn::updateCustomerPayment() noexcept {
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".customer "
                     "SET c_balance = c_balance - ?, "
                     "c_ytd_payment = c_ytd_payment + ?, "
                     "c_payment_cnt = c_payment + 1 "
                     "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? "
                     ";";
  CassIterator* it = nullptr;
  auto payment = static_cast<uint64_t>(payment_ * 100);
  return ycql_impl::execute_write_cql(conn_, stmt, &it, payment, payment, w_id_,
                                      d_id_, c_id_);
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
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLPaymentTxn::getDistrict() noexcept {
  std::string stmt =
      "SELECT d_street_1, d_street_2, d_city, d_state, d_zip "
      "FROM " +
      YCQLKeyspace +
      ".district "
      "WHERE w_id = ? AND d_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_);
  return {st, it};
}

}  // namespace ycql_impl