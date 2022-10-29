#include "ysql_impl//sql_txn/popular_item_txn.h"

#include <pqxx/pqxx>
#include <thread>

#include "common/util/string_util.h"

namespace ydb_util {
Status YSQLPopularItemTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Popular items Transaction started";
  auto InputString = format("I %d %d %d", w_id_, d_id_,l_);

  auto start = std::chrono::system_clock::now();
  int retryCount = 0;

  while (retryCount < MAX_RETRY_COUNT) {
    try {
      std::map<std::string, int> popularItems;
      outputs.push_back(
          format("District identifier:(%d,%d)\nNumber of last orders to be "
                 "examined:%d",
                 w_id_, d_id_, l_));
      pqxx::work txn(*conn_);
      std::string nxtOrderQuery = format(
          "SELECT d_next_o_id FROM district WHERE d_w_id = %d AND d_id = %d",
          w_id_, d_id_);
      LOG_INFO << nxtOrderQuery;
      pqxx::result nxtOrders = txn.exec(nxtOrderQuery);
      if (nxtOrders.empty()) {
        throw std::runtime_error("[popular items error]: order not found");
      }
      auto next_order_id = nxtOrders[0]["d_next_o_id"].c_str();
      // Get last orders
      std::string lastOrderQuery = format(
          "SELECT o_id, o_c_id, o_entry_d FROM orders WHERE o_w_id = %d AND "
          "o_d_id = %d AND o_id >= %s-%d AND o_id < %s",
          w_id_, d_id_, next_order_id, l_, next_order_id);
      LOG_INFO << lastOrderQuery;
      pqxx::result orders = txn.exec(lastOrderQuery);
      LOG_INFO << "Order's Size is " << orders.size();
      for (auto order : orders) {
        auto o_id = order["o_id"].c_str();
        auto c_id = order["o_c_id"].c_str();
        auto o_entry_d = order["o_entry_d"].c_str();
        outputs.push_back(
            format("  (a).Order number:(%s) & entry date and time (%s)", o_id,
                   o_entry_d));
        auto customQuery = format(
            "SELECT c_first, c_middle, c_last FROM customer WHERE c_w_id = %d "
            "AND c_d_id = %d AND c_id = %s",
            w_id_, d_id_, c_id);
        LOG_INFO << customQuery;
        pqxx::result customer = txn.exec(customQuery);
        if (customer.empty()) {
          throw std::runtime_error("[popular items error]: customer not found");
        } else if (customer.size() > 1) {
          throw std::runtime_error(
              "[popular items error]: more than one customer");
        }
        auto firstName = customer[0]["c_first"].c_str();
        auto middleName = customer[0]["c_middle"].c_str();
        auto lastName = customer[0]["c_last"].c_str();
        outputs.push_back(
            format("  (b).Name of customer who placed this order (%s, %s, %s)",
                   firstName, middleName, lastName));
        auto MaxOrderLinesQuery = format(
            "SELECT ol_i_id, max(ol_quantity) as max_ol_quantity FROM "
            "orderline "
            "WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %s group by "
            "ol_i_id",
            w_id_, d_id_, o_id);
        LOG_INFO << MaxOrderLinesQuery;
        pqxx::result orderLines = txn.exec(MaxOrderLinesQuery);
        if (!orderLines.empty()) {
          outputs.push_back(format("  (c).For each popular item in:"));
          for (auto orderLine : orderLines) {
            auto ol_quantity = orderLine["max_ol_quantity"].c_str();
            auto item_id = orderLine["ol_i_id"].c_str();
            auto item = txn.exec(
                format("SELECT i_name FROM item WHERE i_id = %s", item_id));
            if (item.empty()) {
              throw std::runtime_error("[popular items error]: item not found");
            }
            auto itemName = item[0]["i_name"].c_str();
            popularItems[itemName] += 1;
            outputs.push_back(
                format("    (I).Item Name:%s\n    (II).Quantity ordered:%s\n",
                       itemName, ol_quantity));
          }
        }
      }
      // print The percentage of examined orders that contain each popular item
      outputs.push_back(format("For each distinct popular item:\n"));
      for (auto& popularItem : popularItems) {
        auto itemName = popularItem.first.c_str();
        auto Percentage = 1.0 * popularItem.second / orders.size();
        outputs.push_back(format("  (i)item name:%s (ii)The percentage: %f",
                                 itemName, Percentage));
      }
      txn.commit();
      break;
    } catch (const std::exception& e) {
      retryCount++;
      LOG_ERROR << e.what();
      if (retryCount == MAX_RETRY_COUNT) {
        err_out_ << InputString << std::endl;
        err_out_ << e.what() << "\n";
      }
      LOG_INFO << "Retry time:" << retryCount;
      if (!outputs.empty()) {
        std::vector<std::string>().swap(outputs);  // clean the memory
      }
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
  auto end = std::chrono::system_clock::now();
  *diff_t = (end-start).count();
  return Status::OK();
}
}  // namespace ydb_util