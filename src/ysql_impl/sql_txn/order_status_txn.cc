#include "ysql_impl//sql_txn/order_status_txn.h"

#include <pqxx/pqxx>

#include "common/util/string_util.h"
#include "thread"

namespace ydb_util {
Status YSQLOrderStatusTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Order Status Transaction started";

  auto start = std::chrono::system_clock::now();
  int retryCount = 0;
  auto OrderStatusInput = format("O %d %d %d", c_w_id_, c_d_id_, c_id_);

  while (retryCount < MAX_RETRY_COUNT) {
    try {
      pqxx::work txn(*conn_);
      txn.exec(format("set yb_transaction_priority_lower_bound = %f",
                      retryCount * 0.2));
      SQL_Output_Customer_Name(c_w_id_, c_d_id_, c_id_, &txn);
      int O_ID = SQL_Get_Last_O_ID(c_w_id_, c_d_id_, c_id_, &txn);
      SQL_Get_Item(c_w_id_, c_d_id_, O_ID, &txn);

      auto end = std::chrono::system_clock::now();
      *diff_t = (end - start).count();
      txn.commit();
      txn_out_ << OrderStatusInput << std::endl;
      for (auto& output : outputs) {
        txn_out_ << output << std::endl;
      }
      return Status::OK();

    } catch (const std::exception& e) {
      retryCount++;
      LOG_ERROR << e.what();
      if (retryCount == MAX_RETRY_COUNT) {
        err_out_ << OrderStatusInput << std::endl;
        err_out_ << e.what() << "\n";
      }
      int randRetryTime = rand() % 100 + 1;
      std::this_thread::sleep_for(
          std::chrono::milliseconds((100 + randRetryTime) * retryCount));
    }
  }
  return Status::Invalid("retry times exceeded max retry count");
}

void YSQLOrderStatusTxn::SQL_Output_Customer_Name(int c_w_id, int c_d_id,
                                                  int c_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get Customer Name:";
  std::string query = format(
      "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM Customer WHERE C_W_ID "
      "= %d AND "
      "C_D_ID = %d AND C_ID = %d",
      c_w_id, c_d_id, c_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("D_Next_O_ID not found");
  }

  for (auto row : res) {
    outputs.push_back(format("C_FIRST: %s, C_MIDDLE: %s, C_LAST: %s",
                             row["C_FIRST"].c_str(), row["C_MIDDLE"].c_str(),
                             row["C_LAST"].c_str()));
    //    std::cout << "C_FIRST: " << row["C_FIRST"].c_str() << ", "
    //              << "C_MIDDLE: " << row["C_MIDDLE"].c_str() << ", "
    //              << "C_LAST: " << row["C_LAST"].c_str() << std::endl;
  }
}

int YSQLOrderStatusTxn::SQL_Get_Last_O_ID(int o_w_id, int o_d_id, int o_c_id,
                                          pqxx::work* txn) {
  LOG_INFO << ">>>> Get Last O_ID:";
  pqxx::result res;
  std::string query = format(
      "SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM orders WHERE O_W_ID = %d AND "
      "O_D_ID = %d AND O_C_ID = %d ORDER BY O_ID DESC LIMIT 1",
      o_w_id, o_d_id, o_c_id);
  LOG_INFO << query;
  res = txn->exec(query);

  for (auto row : res) {
    outputs.push_back(format("O_ID: %d, O_ENTRY_D: %s, O_CARRIER_ID: %s",
                             row["O_ID"].as<int>(), row["O_ENTRY_D"].c_str(),
                             row["O_CARRIER_ID"].c_str()));
    //    std::cout << "O_ID: " << row["O_ID"].as<int>() << ", "
    //              << "O_ENTRY_D: " << row["O_ENTRY_D"].c_str() << ", "
    //              << "O_CARRIER_ID: " << row["O_CARRIER_ID"].as<int>() <<
    //              std::endl;
  }
  return res[0]["O_ID"].as<int>();
}

void YSQLOrderStatusTxn::SQL_Get_Item(int ol_w_id, int ol_d_id, int ol_o_id,
                                      pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get items in the last order:";

  std::string query = format(
      "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D "
      "FROM orderline WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d",
      ol_w_id, ol_d_id, ol_o_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("Items not found");
  }

  for (auto row : res) {
    outputs.push_back(
        format("OL_I_ID: %d, OL_SUPPLY_W_ID: %d, OL_QUANTITY: %d, OL_AMOUNT: "
               "%f, OL_DELIVERY_D: %s",
               row["OL_I_ID"].as<int>(), row["OL_SUPPLY_W_ID"].as<int>(),
               row["OL_QUANTITY"].as<int>(), row["OL_AMOUNT"].as<float>(),
               row["OL_DELIVERY_D"].c_str()));
    //    std::cout << "OL_I_ID: " << row["OL_I_ID"].as<int>() << ", "
    //              << "OL_SUPPLY_W_ID: " << row["OL_SUPPLY_W_ID"].as<int>() <<
    //              ", "
    //              << "OL_QUANTITY: " << row["OL_QUANTITY"].as<int>() << ", "
    //              << "OL_AMOUNT: " << row["OL_AMOUNT"].as<float>() << ", "
    //              << "OL_DELIVERY_D: " << row["OL_DELIVERY_D"].c_str() <<
    //              std::endl;
  }
}

}  // namespace ydb_util