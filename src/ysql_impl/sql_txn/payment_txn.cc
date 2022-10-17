#include "ysql_impl//sql_txn/payment_txn.h"

#include <pqxx/pqxx>

#include "common/util/string_util.h"

namespace ydb_util {
Status YSQLPaymentTxn::Execute() noexcept {
  LOG_INFO << "Payment Transaction started";
  int retryCount = 0;
  while (retryCount < MAX_RETRY_COUNT) {
    try {
      pqxx::work txn(*conn_);
      pqxx::row warehouse = getWarehouseSQL_(w_id_, &txn);
      double old_w_ytd = std::stod(warehouse["w_ytd"].c_str());
      double new_w_ytd = old_w_ytd + payment_;
      updateWareHouseSQL_(w_id_, old_w_ytd, new_w_ytd, &txn);
      pqxx::row district = getDistrictSQL_(w_id_, d_id_, &txn);
      double old_d_ytd = std::stod(district["d_ytd"].c_str());
      double new_d_ytd = old_w_ytd + payment_;
      updateDistrictSQL_(w_id_, d_id_, old_d_ytd, new_d_ytd, &txn);
      std::string UpdateCustomerSQL = format(
          "UPDATE Customer SET C_BALANCE = C_BALANCE - %s, C_YTD_PAYMENT = "
          "C_YTD_PAYMENT + %s,C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE "
          "C_W_ID = %s AND C_D_ID = %s AND C_ID =%s ",
          std::to_string(payment_).c_str(), std::to_string(payment_).c_str(),
          std::to_string(w_id_).c_str(), std::to_string(d_id_).c_str(),
          std::to_string(c_id_).c_str());
      txn.exec(UpdateCustomerSQL);
      std::string getCustomerSQL = format(
          "SELECT * FROM Customer WHERE C_W_ID = %s AND C_D_ID = %s AND "
          "C_ID = %s",
          std::to_string(w_id_).c_str(), std::to_string(d_id_).c_str(),
          std::to_string(c_id_).c_str());
      pqxx::result customers = txn.exec(getCustomerSQL);
      if (customers.empty()) {
        throw std::runtime_error("Customer not found");
      }
      pqxx::row customer=customers[0];
      txn.commit();
      std::cout << "Customer Information:\n "
                << "identifier (C_W_ID,C_D_ID,C_ID)=(" << customer["c_w_id"].c_str()<<","<<customer["c_d_id"].c_str()<<","<<customer["c_id"].c_str() << "),\n "
                << "name (C_FIRST,C_MIDDLE,C_LAST)=(" << customer["c_first"].c_str()<<","<<customer["c_middle"].c_str()<<","<<customer["c_last"].c_str() << "),\n "
                << "address (C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP)=(" << customer["C_STREET_1"].c_str() <<","<< customer["C_STREET_2"].c_str()
                <<","<<customer["C_CITY"].c_str()<<","<<customer["C_STATE"].c_str() <<","<< customer["C_ZIP"].c_str() << ")\n "
                << "C_PHONE=" << customer["C_PHONE"].c_str()<<","
                << "C_SINCE=" << customer["C_SINCE"].c_str() << ", "
                << "C_CREDIT=" << customer["C_CREDIT"].c_str()<<", "
                << "C_CREDIT_LIM=" << customer["C_CREDIT_LIM"].c_str() << ", "
                << "C_DISCOUNT=" << customer["C_DISCOUNT"].c_str() << ", "
                << "C_BALANCE=" << customer["C_BALANCE"].as<float>()-payment_<<std::endl;

      std::cout << "District Information:\n "
                << "d_street_1=" << district["d_street_1"].c_str() << ", "
               << "d_street_2=" << district["d_street_2"].c_str() << ", "
               << "d_city=" << district["d_city"].c_str() << ", "
               << "d_state=" << district["d_state"].c_str()<< ", "
               << "d_zip=" << district["d_zip"].c_str() << ", "
               << "d_ytd=" << district["d_ytd"].as<float>()<<std::endl;

      std::cout << "Warehouse Information:\n "
                <<"w_street_1=" << warehouse["w_street_1"].c_str() << ", "
               << "w_street_2=" << warehouse["w_street_2"].c_str() << ", "
               << "w_city=" << warehouse["w_city"].c_str() << ", "
               << "w_state=" << warehouse["w_state"].c_str()<< ", "
               << "w_zip=" << warehouse["w_zip"].c_str() << ", "
               << "w_ytd=" << warehouse["w_ytd"].as<float>()<<std::endl;
      std::cout<<"Payment:"<< payment_<< std::endl;

      return Status::OK();
    } catch (const std::exception& e) {
      retryCount++;
      LOG_ERROR << e.what();
      LOG_INFO << "Retry time:"<<retryCount;
      // if Failed, Wait for 100 ms to try again
      std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
    }
  }
  return Status::Invalid("retry times exceeded max retry count");
}

void YSQLPaymentTxn::updateWareHouseSQL_(int w_id, double old_w_ytd,
                                         double w_ytd, pqxx::work* txn) {
  std::string query = format("UPDATE warehouse set w_ytd = %.2f  where w_id = %d and w_ytd = %.2f ;"
                             ,w_ytd, w_id,old_w_ytd);
//  conn->prepare("updateWareHouse",
//                "UPDATE warehouse "
//                "SET w_ytd = $1"
//                "WHERE w_id = $2 and w_ytd = $3;");
  pqxx::result contests =
      //txn->exec_prepared("updateWareHouse", w_ytd, w_id, old_w_ytd);
      txn->exec(query);
}

void YSQLPaymentTxn::updateDistrictSQL_(int w_id, int d_id, double old_d_ytd,
                                        double d_ytd, pqxx::work* txn) {
//  pqxx::connection* conn = &txn->conn();
//  conn->prepare("updateDistrict",
//                "UPDATE district "
//                "SET d_ytd = $1"
//                "WHERE d_w_id = $2 AND d_id= $3 and d_ytd = $4;");
//  pqxx::result contests =
//      txn->exec_prepared("updateDistrict", d_ytd, w_id, d_id, old_d_ytd);
    std::string query = format("UPDATE district set d_ytd = %.2f  where d_w_id = %d and d_id = %d and d_ytd = %.2f;"
                           ,d_ytd, w_id, d_id, old_d_ytd);
    txn->exec(query);
}

pqxx::row YSQLPaymentTxn::getDistrictSQL_(int w_id, int d_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get District:";
  std::string query = format(
      "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd FROM "
      "district WHERE d_w_id = %d AND d_id = %d",
      w_id, d_id);
  LOG_INFO << query;
  res = txn->exec(query);
  if (res.empty()) {
    throw std::runtime_error("District not found");
  }
//  for (auto row : res) {
//    LOG_INFO << "d_street_1=" << row["d_street_1"].c_str() << ", "
//             << "d_street_2=" << row["d_street_2"].c_str() << ", "
//             << "d_city=" << row["d_city"].c_str() << ", "
//             << "d_state=" << row["d_state"].c_str()
//             << "d_zip=" << row["d_zip"].c_str() << ", "
//             << "d_ytd=" << row["d_ytd"].as<float>();
//  }
  return res[0];
}

pqxx::row YSQLPaymentTxn::getWarehouseSQL_(int w_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get Warehouse:";
  std::string query = format(
      "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd FROM "
      "warehouse WHERE w_id = %d",
      w_id);
  LOG_INFO << query;
  res = txn->exec(query);
  if (res.empty()) {
    throw std::runtime_error("Warehouse not found");
  }
//  for (auto row : res) {
//    LOG_INFO << "w_street_1=" << row["w_street_1"].c_str() << ", "
//             << "w_street_2=" << row["w_street_2"].c_str() << ", "
//             << "w_city=" << row["w_city"].c_str() << ", "
//             << "w_state=" << row["w_state"].c_str()
//             << "w_zip=" << row["w_zip"].c_str() << ", "
//             << "w_ytd=" << row["w_ytd"].as<float>();
//  }
  return res[0];
}
}  // namespace ydb_util