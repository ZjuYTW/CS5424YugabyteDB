#include "ysql_impl//sql_txn/stock_level_txn.h"

#include <pqxx/pqxx>

#include "common/util/string_util.h"

#include "thread"

namespace ydb_util {
float YSQLStockLevelTxn::Execute() noexcept {
  LOG_INFO << "Stock Level Transaction started";

  time_t start_t, end_t;
  double diff_t;
  time(&start_t);
  int retryCount = 0;

  while (retryCount < MAX_RETRY_COUNT) {
    try {
      pqxx::work txn(*conn_);
      int d_next_o_id = SQL_Get_D_Next_O_ID(w_id_, d_id_, &txn);

      auto items = SQL_Get_OL_I_ID(w_id_, d_id_, d_next_o_id, &txn);

      int items_below_threshold = 0;
      for (int i = 0; i < items.size(); i++) {
        items_below_threshold += SQL_check_stock(w_id_, items[i]["OL_I_ID"].as<int>(), &txn);
      }
//      std::cout << "Total number of items in S where its stock quantity at W_ID is below the threshold: "
//                << items_below_threshold << std::endl;
      outputs.push_back(format("Total number of items in S where its stock quantity at W_ID is below the threshold: %d",
                               items_below_threshold));

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

int YSQLStockLevelTxn::SQL_Get_D_Next_O_ID(int w_id, int d_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get D_Next_O_ID:";
  std::string query = format(
      "SELECT D_Next_O_ID FROM District WHERE D_W_ID = %d AND D_ID = %d",
      w_id, d_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("D_Next_O_ID not found");
  }

  return res[0]["D_Next_O_ID"].as<int>();
}

pqxx::result YSQLStockLevelTxn::SQL_Get_OL_I_ID(int ol_w_id, int ol_d_id, int d_next_o_id, pqxx::work* txn) {
  LOG_INFO << ">>>> Get OL_I_ID:";
  pqxx::result res;
  std::string query = format(
      "SELECT DISTINCT OL_I_ID FROM orderline WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID >= %d AND OL_O_ID < %d",
      ol_w_id, ol_d_id, d_next_o_id-l_, d_next_o_id);
  LOG_INFO << query;
  res = txn->exec(query);

  return res;
}

int YSQLStockLevelTxn::SQL_check_stock(int s_w_id, int s_i_id, pqxx::work* txn) {
  LOG_INFO << ">>>> Check stock:";
  pqxx::result res;
  std::string query = format(
      "SELECT S_QUANTITY FROM stock WHERE s_w_id = %d AND s_i_id = %d",
      s_w_id, s_i_id);
  LOG_INFO << query;
  res = txn->exec(query);

  int ret = 0;
  if (res[0]["S_QUANTITY"].as<int>() < t_) {
    ret = 1;
  }
  return ret;
}

}  // namespace ydb_util