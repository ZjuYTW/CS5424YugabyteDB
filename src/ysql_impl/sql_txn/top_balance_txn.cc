#include "ysql_impl/sql_txn/top_balance_txn.h"

#include <pqxx/pqxx>

namespace ydb_util {
Status YSQLTopBalanceTxn::Execute() noexcept {
  LOG_INFO << "Top-Balance Transaction started";
  pqxx::work txn(*conn_);
  int retryCount = 0;
  while (retryCount < MAX_RETRY_COUNT) {
    try {
      LOG_INFO << "Start Executing!";
      std::string getTopBalanceSQL = format(
          "SELECT c_w_id, c_d_id, c_balance, c_first, c_middle, c_last "
          "FROM customer "
          "ORDER BY c_balance DESC "
          "LIMIT %s;",
          std::to_string(TOP_K).c_str());
      pqxx::result customers = txn.exec(getTopBalanceSQL);
      for (auto customer : customers) {
        int w_id = customer["c_w_id"].as<int>();
        int d_id = customer["c_d_id"].as<int>();
        pqxx::row warehouse = getWarehouseSQL_(w_id, &txn);
        pqxx::row district = getDistrictSQL_(w_id, d_id, &txn);

        LOG_INFO << "W_ID: " << w_id << ", D_ID: " << d_id
                 << ", C_BALANCE: " << customer["c_balance"].as<double>()
                 << ", C_FIRST: " << customer["c_first"].as<std::string>()
                 << ", C_MIDDLE: " << customer["c_middle"].as<std::string>()
                 << ", C_LAST: " << customer["c_last"].as<std::string>()
                 << ", W_NAME: " << warehouse["w_name"].as<std::string>()
                 << ", D_NAME: " << district["d_name"].as<std::string>();
      }
      LOG_INFO << "Got Result!";
      if (customers.empty()) {
        throw std::runtime_error("Top Balance Customers not found");
      }
      txn.commit();
      return Status::OK();
    } catch (const std::exception& e) {
      retryCount++;
      LOG_ERROR << e.what();
      // if Failed, Wait for 100 ms to try again
      std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
    }
  }
  return Status::Invalid("retry times exceeded max retry count");
}

pqxx::row YSQLTopBalanceTxn::getWarehouseSQL_(int w_id, pqxx::work* txn) {
  std::string getWarehouseSQL = format(
      "SELECT * FROM warehouse WHERE w_id = %s", std::to_string(w_id).c_str());
  pqxx::result result = txn->exec(getWarehouseSQL);
  if (result.empty()) {
    throw std::runtime_error("Warehouse not found");
  }
  return result[0];
}

pqxx::row YSQLTopBalanceTxn::getDistrictSQL_(int w_id, int d_id,
                                             pqxx::work* txn) {
  std::string getDistrictSQL =
      format("SELECT * FROM district WHERE d_w_id = %s AND d_id = %s",
             std::to_string(w_id).c_str(), std::to_string(d_id).c_str());
  pqxx::result result = txn->exec(getDistrictSQL);
  if (result.empty()) {
    throw std::runtime_error("District not found");
  }
  return result[0];
}
}  // namespace ydb_util