#include "ysql_impl//sql_txn/payment_txn.h"

#include <pqxx/pqxx>

#include "common/util/string_util.h"

namespace ydb_util {
Status YSQLPaymentTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Payment Transaction started";
  auto InputString = format("P %d %d %d", w_id_, d_id_, c_id_);
  auto start = std::chrono::system_clock::now();
  int retryCount = 0;

  while (retryCount < MAX_RETRY_COUNT) {
    pqxx::nontransaction l_work(*conn_);
    try {
      l_work.exec(format("set yb_transaction_priority_lower_bound = %f;",
                         retryCount * 0.2));
      l_work.exec("begin TRANSACTION;");
      pqxx::row warehouse = getWarehouseSQL_(w_id_, &l_work);
      double old_w_ytd = std::stod(warehouse["w_ytd"].c_str());
      double new_w_ytd = old_w_ytd + payment_;
      updateWareHouseSQL_(w_id_, old_w_ytd, new_w_ytd, &l_work);
      pqxx::row district = getDistrictSQL_(w_id_, d_id_, &l_work);
      double old_d_ytd = std::stod(district["d_ytd"].c_str());
      double new_d_ytd = old_w_ytd + payment_;
      updateDistrictSQL_(w_id_, d_id_, old_d_ytd, new_d_ytd, &l_work);
      std::string UpdateCustomerSQL = format(
          "UPDATE Customer SET C_BALANCE = C_BALANCE - %s, C_YTD_PAYMENT = "
          "C_YTD_PAYMENT + %s,C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE "
          "C_W_ID = %s AND C_D_ID = %s AND C_ID =%s ",
          std::to_string(payment_).c_str(), std::to_string(payment_).c_str(),
          std::to_string(w_id_).c_str(), std::to_string(d_id_).c_str(),
          std::to_string(c_id_).c_str());
      l_work.exec(UpdateCustomerSQL);
      std::string getCustomerSQL = format(
          "SELECT * FROM Customer WHERE C_W_ID = %s AND C_D_ID = %s AND "
          "C_ID = %s",
          std::to_string(w_id_).c_str(), std::to_string(d_id_).c_str(),
          std::to_string(c_id_).c_str());
      pqxx::result customers = l_work.exec(getCustomerSQL);
      if (customers.empty()) {
        throw std::runtime_error("Customer not found");
      }
      pqxx::row customer = customers[0];
      l_work.exec("commit;");
      l_work.exec(format("set yb_transaction_priority_lower_bound = 0;"));
      outputs.push_back("Customer Information:");
      outputs.push_back(format("identifier (C_W_ID,C_D_ID,C_ID)=(%s, %s, %s)",
                               customer["c_w_id"].c_str(),
                               customer["c_d_id"].c_str(),
                               customer["c_id"].c_str()));
      outputs.push_back(format("name (C_FIRST,C_MIDDLE,C_LAST)=(%s, %s, %s)",
                               customer["c_first"].c_str(),
                               customer["c_middle"].c_str(),
                               customer["c_last"].c_str()));
      outputs.push_back(
          format("address (C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP)=(%s, "
                 "%s, %s, %s, %s)",
                 customer["C_STREET_1"].c_str(), customer["C_STREET_2"].c_str(),
                 customer["C_CITY"].c_str(), customer["C_STATE"].c_str(),
                 customer["C_ZIP"].c_str()));
      outputs.push_back(format(
          "C_PHONE=%s, C_SINCE=%s, C_CREDIT=%s, C_CREDIT_LIM=%s, "
          "C_DISCOUNT=%s, C_BALANCE=%f",
          customer["C_PHONE"].c_str(), customer["C_SINCE"].c_str(),
          customer["C_CREDIT"].c_str(), customer["C_CREDIT_LIM"].c_str(),
          customer["C_DISCOUNT"].c_str(), customer["C_BALANCE"].as<float>()));
      ;
      outputs.push_back("District Information:");
      outputs.push_back(
          format("d_street_1=%s, d_street_2=%s, d_city=%s, d_state=%s, "
                 "d_zip=%s, d_ytd=%f",
                 district["d_street_1"].c_str(), district["d_street_2"].c_str(),
                 district["d_city"].c_str(), district["d_state"].c_str(),
                 district["d_zip"].c_str(), district["d_ytd"].as<float>()));
      outputs.push_back("Warehouse Information:");
      outputs.push_back(format(
          "w_street_1=%s, w_street_2=%s, w_city=%s, w_state=%s, "
          "w_zip=%s, w_ytd=%f",
          warehouse["w_street_1"].c_str(), warehouse["w_street_2"].c_str(),
          warehouse["w_city"].c_str(), warehouse["w_state"].c_str(),
          warehouse["w_zip"].c_str(), warehouse["w_ytd"].as<float>()));
      outputs.push_back(format("Payment: %f", payment_));
      //      std::cout << "Payment:" << payment_ << std::endl;

      auto end = std::chrono::system_clock::now();
      *diff_t = (end - start).count();
      txn_out_ << InputString << std::endl;
      for (auto& output : outputs) {
        txn_out_ << output << std::endl;
      }
      return Status::OK();

    } catch (const std::exception& e) {
      l_work.exec("ROLLBACK;");
      retryCount++;
      LOG_ERROR << e.what();
      if (retryCount == MAX_RETRY_COUNT) {
        err_out_ << InputString << std::endl;
        err_out_ << e.what() << "\n";
      }
      LOG_INFO << "Retry time:" << retryCount;
      // if Failed, Wait for 100 ms to try again
      int randRetryTime = rand() % 100 + 1;
      std::this_thread::sleep_for(
          std::chrono::milliseconds((100 + randRetryTime) * retryCount));
    }
  }

  return Status::Invalid("retry times exceeded max retry count");
}

void YSQLPaymentTxn::updateWareHouseSQL_(int w_id, double old_w_ytd,
                                         double w_ytd,
                                         pqxx::nontransaction* l_work) {
  std::string query = format(
      "UPDATE warehouse set w_ytd = %.2f  where w_id = %d and w_ytd = %.2f ;",
      w_ytd, w_id, old_w_ytd);
  pqxx::result contests = l_work->exec(query);
}

void YSQLPaymentTxn::updateDistrictSQL_(int w_id, int d_id, double old_d_ytd,
                                        double d_ytd,
                                        pqxx::nontransaction* l_work) {
  std::string query = format(
      "UPDATE district set d_ytd = %.2f  where d_w_id = %d and d_id = %d and "
      "d_ytd = %.2f;",
      d_ytd, w_id, d_id, old_d_ytd);
  l_work->exec(query);
}

pqxx::row YSQLPaymentTxn::getDistrictSQL_(int w_id, int d_id,
                                          pqxx::nontransaction* l_work) {
  pqxx::result res;
  LOG_INFO << ">>>> Get District:";
  std::string query = format(
      "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd FROM "
      "district WHERE d_w_id = %d AND d_id = %d",
      w_id, d_id);
  LOG_INFO << query;
  res = l_work->exec(query);
  if (res.empty()) {
    throw std::runtime_error("District not found");
  }
  return res[0];
}

pqxx::row YSQLPaymentTxn::getWarehouseSQL_(int w_id,
                                           pqxx::nontransaction* l_work) {
  pqxx::result res;
  LOG_INFO << ">>>> Get Warehouse:";
  std::string query = format(
      "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd FROM "
      "warehouse WHERE w_id = %d",
      w_id);
  LOG_INFO << query;
  res = l_work->exec(query);
  if (res.empty()) {
    throw std::runtime_error("Warehouse not found");
  }
  return res[0];
}
}  // namespace ydb_util