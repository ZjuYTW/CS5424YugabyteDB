#include "ysql_impl//sql_txn/order_status_txn.h"

#include <pqxx/pqxx>

#include "common/util/string_util.h"

#include "thread"

namespace ydb_util {
float YSQLOrderStatusTxn::Execute() noexcept {
  LOG_INFO << "Order Status Transaction started";

  time_t start_t, end_t;
  double diff_t;
  time(&start_t);
  int retryCount = 0;

  while (retryCount < MAX_RETRY_COUNT) {
    try {
      pqxx::work txn(*conn_);
      SQL_Output_Customer_Name(c_w_id_, c_d_id_, c_id_, &txn);
      int O_ID = SQL_Get_Last_O_ID(c_w_id_, c_d_id_, c_id_, &txn);
      SQL_Get_Item(c_w_id_, c_d_id_, O_ID, &txn);

      time(&end_t);
      diff_t = difftime(end_t, start_t);
      return diff_t;

    } catch (const std::exception& e) {
      retryCount++;
      LOG_ERROR << e.what();
      // if Failed, Wait for 100 ms to try again
      // TODO: check if there is a sleep_for
      std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
    }
  }
  return 0;
}

void YSQLOrderStatusTxn::SQL_Output_Customer_Name(int c_w_id, int c_d_id, int c_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get Customer Name:";
  std::string query = format(
      "SELECT C_FIRST, C_MIDDLE, C_LAST FROM Customer WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d",
      c_w_id, c_d_id, c_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("D_Next_O_ID not found");
  }

  for (auto row : res) {
    std::cout << "C_FIRST: " << row["C_FIRST"].c_str() << ", "
              << "C_MIDDLE: " << row["C_MIDDLE"].c_str() << ", "
              << "C_LAST: " << row["C_LAST"].c_str() << std::endl;
  }
}

int YSQLOrderStatusTxn::SQL_Get_Last_O_ID(int o_w_id, int o_d_id, int o_c_id, pqxx::work* txn) {
  LOG_INFO << ">>>> Get Last O_ID:";
  pqxx::result res;
  std::string query = format(
          "SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM orders WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d ORDER BY O_ID DESC LIMIT 1",
          o_w_id, o_d_id, o_c_id);
  LOG_INFO << query;
  res = txn->exec(query);

  for (auto row : res) {
    std::cout << "O_ID: " << row["O_ID"].as<int>() << ", "
              << "O_ENTRY_D: " << row["O_ENTRY_D"].c_str() << ", "
              << "O_CARRIER_ID: " << row["O_CARRIER_ID"].as<int>() << std::endl;
  }
  return res[0]["O_ID"].as<int>();
}

void YSQLOrderStatusTxn::SQL_Get_Item(int ol_w_id, int ol_d_id, int ol_o_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get items in the last order:";

  std::string query = format(
          "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM orderline WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d",
      ol_w_id, ol_d_id, ol_o_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("Items not found");
  }

  for (auto row: res) {
    std::cout << "OL_I_ID: " << row["OL_I_ID"].as<int>() << ", "
              << "OL_SUPPLY_W_ID: " << row["OL_SUPPLY_W_ID"].as<int>() << ", "
              << "OL_QUANTITY: " << row["OL_QUANTITY"].as<int>() << ", "
              << "OL_AMOUNT: " << row["OL_AMOUNT"].as<float>() << ", "
              << "OL_DELIVERY_D: " << row["OL_DELIVERY_D"].c_str() << std::endl;
  }
}

}  // namespace ydb_util