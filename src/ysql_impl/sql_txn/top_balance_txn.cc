#include "ysql_impl/sql_txn/top_balance_txn.h"

#include <pqxx/pqxx>

namespace ydb_util {
Status YSQLTopBalanceTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Top-Balance Transaction started";
  auto InputString = format("T");
  time_t start_t, end_t;
  time(&start_t);
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

        outputs.push_back(format(
            "(a) Name of Customer: (%s, %s, %s)", customer["c_first"].c_str(),
            customer["c_middle"].c_str(), customer["c_last"].c_str()));
        outputs.push_back(format("(b) Customer's Balance: %s",
                                 customer["c_balance"].c_str()));
        outputs.push_back(format("(c) Customer's Warehouse: %s",
                                 warehouse["w_name"].c_str()));
        outputs.push_back(
            format("(d) Customer's District: %s", district["d_name"].c_str()));
      }
      if (customers.empty()) {
        throw std::runtime_error("Top Balance Customers not found");
      }
      txn.commit();
      break;
    } catch (const std::exception& e) {
      retryCount++;
      if (retryCount == MAX_RETRY_COUNT) {
        err_out_ << InputString << std::endl;
        err_out_ << e.what() << "\n";
      }
      LOG_ERROR << e.what();
      if (!outputs.empty()) {
        std::vector<std::string>().swap(outputs);  // clean the memory
      }
      // if Failed, Wait for 100 ms to try again
      std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
    }
  }
  if (retryCount == MAX_RETRY_COUNT) {
    return Status::Invalid("retry times exceeded max retry count");
  }
  txn_out_<<InputString<<std::endl;
  for (auto& output : outputs) {
    txn_out_ << output << std::endl;
  }
  time(&end_t);
  *diff_t = difftime(end_t, start_t);
  return Status::OK();
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