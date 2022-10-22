#include "ycql_impl/cql_txn/payment_txn.h"
#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLPaymentTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Payment Transaction started";
  int retry_time = 0;
  bool done = false;
  auto st = Status::OK();
  CassIterator *customer_it = nullptr, *warehouse_it = nullptr, *district_it = nullptr;

  do {
    st = updateWarehouseYTD();
    if (!st.ok()) continue;
    st = updateDistrictYTD();
    if (!st.ok()) continue;
    st = updateCustomerPayment();
    if (!st.ok()) continue;

    if (!customer_it) {
      std::tie(st, customer_it) = getCustomer();
      if (!st.ok()) continue;
    }
    std::cout << "\t(a).Customer information:" << std::endl;
    std::cout << format("\t\tIdentifier: (%d, %d, %d)", w_id_, d_id_, c_id_) << std::endl;
    std::cout << format("\t\tName: (%s, %s, %s)",
                        GetValueFromCassRow<std::string>(customer_it, "c_first").c_str(),
                        GetValueFromCassRow<std::string>(customer_it, "c_middle").c_str(),
                        GetValueFromCassRow<std::string>(customer_it, "c_last").c_str()) << std::endl;
    std::cout << format("\t\tAddress: (%s, %s, %s, %s, %s)",
                        GetValueFromCassRow<std::string>(customer_it, "c_street_1").c_str(),
                        GetValueFromCassRow<std::string>(customer_it, "c_street_2").c_str(),
                        GetValueFromCassRow<std::string>(customer_it, "c_city").c_str(),
                        GetValueFromCassRow<std::string>(customer_it, "c_state").c_str(),
                        GetValueFromCassRow<std::string>(customer_it, "c_zip").c_str()) << std::endl;
    std::cout << format("\t\tC_PHONE: %s", GetValueFromCassRow<std::string>(customer_it, "c_phone").c_str()) << std::endl;
    std::cout << format("\t\tC_SINCE: %s", GetValueFromCassRow<std::string>(customer_it, "c_since").c_str()) << std::endl;
    std::cout << format("\t\tC_CREDIT: %s", GetValueFromCassRow<std::string>(customer_it, "c_credit").c_str()) << std::endl;
    std::cout << format("\t\tC_CREDIT_LIM: %lf", GetValueFromCassRow<double>(customer_it, "c_credit_lim")) << std::endl;
    std::cout << format("\t\tC_DISCOUNT: %lf", GetValueFromCassRow<double>(customer_it, "c_discount")) << std::endl;
    std::cout << format("\t\tC_BALANCE: %lf", static_cast<double>(GetValueFromCassRow<uint64_t>(customer_it, "c_balance") / 100.0)) << std::endl;

    if (!warehouse_it) {
      std::tie(st, warehouse_it) = getWarehouse();
      if (!st.ok()) continue;
    }
    std::cout << "\t(b).Warehouse information:" << std::endl;
    std::cout << format("\t\tAddress: (%s, %s, %s, %s, %s)",
                        GetValueFromCassRow<std::string>(warehouse_it, "w_street_1").c_str(),
                        GetValueFromCassRow<std::string>(warehouse_it, "w_street_2").c_str(),
                        GetValueFromCassRow<std::string>(warehouse_it, "w_city").c_str(),
                        GetValueFromCassRow<std::string>(warehouse_it, "w_state").c_str(),
                        GetValueFromCassRow<std::string>(warehouse_it, "w_zip").c_str()) << std::endl;

    if (!district_it) {
      std::tie(st, district_it) = getDistrict();
      if (!st.ok()) continue;
    }
    std::cout << "\t(c).District information:" << std::endl;
    std::cout << format("\t\tAddress: (%s, %s, %s, %s, %s)",
                        GetValueFromCassRow<std::string>(district_it, "d_street_1").c_str(),
                        GetValueFromCassRow<std::string>(district_it, "d_street_2").c_str(),
                        GetValueFromCassRow<std::string>(district_it, "d_city").c_str(),
                        GetValueFromCassRow<std::string>(district_it, "d_state").c_str(),
                        GetValueFromCassRow<std::string>(district_it, "d_zip").c_str()) << std::endl;

    done = true;
  } while (retry_time++ < MaxRetryTime /* && sleep(done) */);
  if (customer_it) cass_iterator_free(customer_it);
  if (warehouse_it) cass_iterator_free(warehouse_it);
  if (district_it) cass_iterator_free(district_it);
  return st;
}

Status YCQLPaymentTxn::updateWarehouseYTD() {
  std::string stmt =
      "UPDATE warehouse "
      "SET w_ytd = w_ytd + ? "
      "WHERE w_id = ?;";
  CassIterator* it = nullptr;
  auto payment = static_cast<uint64_t>(payment_ * 100);
  return ycql_impl::execute_write_cql(conn_, stmt, &it, payment, w_id_);
}

Status YCQLPaymentTxn::updateDistrictYTD() {
  std::string stmt =
      "UPDATE district "
      "SET d_ytd = d_ytd + ? "
      "WHERE w_id = ? AND d_id = ?;";
  CassIterator* it = nullptr;
  auto payment = static_cast<uint64_t>(payment_ * 100);
  return ycql_impl::execute_write_cql(conn_, stmt, &it, payment, w_id_, d_id_);
}

Status YCQLPaymentTxn::updateCustomerPayment() {
  std::string stmt =
      "UPDATE customer "
      "SET c_balance = c_balance - ?, "
        "c_ytd_payment = c_ytd_payment + ?, "
        "c_payment_cnt = c_payment + 1 "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
  CassIterator* it = nullptr;
  auto payment = static_cast<uint64_t>(payment_ * 100);
  return ycql_impl::execute_write_cql(conn_, stmt, &it, payment, payment, w_id_, d_id_, c_id_);
}

std::pair<Status, CassIterator *> YCQLPaymentTxn::getCustomer() {
  std::string stmt =
      "SELECT c_first, c_middle, c_last, "
      "c_street_1, c_street_2, c_city, c_state, c_zip, "
      "c_phone, c_since, c_credit, "
      "c_credit_lim, c_discount, c_balance"
      "FROM customer "
      "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_, c_id_);
  return {st, it};
}

std::pair<Status, CassIterator *> YCQLPaymentTxn::getWarehouse() {
  std::string stmt =
      "SELECT w_street_1, w_street_2, w_city, w_state, w_zip "
      "FROM warehouse "
      "WHERE w_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_);
  return {st, it};
}

std::pair<Status, CassIterator *> YCQLPaymentTxn::getDistrict() {
  std::string stmt =
      "SELECT d_street_1, d_street_2, d_city, d_state, d_zip "
      "FROM district "
      "WHERE w_id = ? AND d_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_);
  return {st, it};
}




}  // namespace ycql_impl