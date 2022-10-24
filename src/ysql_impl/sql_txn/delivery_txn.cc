#include "ysql_impl//sql_txn/delivery_txn.h"

#include <pqxx/pqxx>

#include "common/util/string_util.h"

namespace ydb_util {
Status YSQLDeliveryTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Delivery Transaction started";

  time_t start_t, end_t;
  time(&start_t);

  for (int d_id = 1; d_id <= 10; d_id++) {
    int retryCount = 0;
    while (retryCount < MAX_RETRY_COUNT) {
      try {
        pqxx::work txn(*conn_);
        LOG_INFO << ">>>> Get Order:";
        std::string OrderQuery = format(
            "SELECT MIN(O_ID) as O_ID FROM orders WHERE O_W_ID = %d AND O_D_ID "
            "= %d AND O_CARRIER_ID is NULL",
            w_id_, d_id);
        pqxx::result orders = txn.exec(OrderQuery);
        if (orders.empty()) {
          throw std::runtime_error("delivery: order not found");
        }
        auto order = orders[0];
        auto order_id = order["O_ID"].c_str();
        std::string UpdateOrder = format(
            "UPDATE orders SET o_carrier_id = %d WHERE o_w_id = %d AND o_d_id "
            "= %d AND o_id = %s",
            carrier_id_, w_id_, d_id, order_id);
        LOG_INFO << UpdateOrder;
        txn.exec(UpdateOrder);

        std::string UpdateOrderLine = format(
            "UPDATE orderline SET ol_delivery_d = '%s' WHERE ol_w_id = %d  AND "
            "ol_d_id = %d AND ol_o_id = %s ",
            getLocalTimeString(), w_id_, d_id, order_id);
        LOG_INFO << UpdateOrderLine;
        txn.exec(UpdateOrderLine);

        // update customers
        // get the sum value of ol amount
        std::string GetSumQuery = format(
            "SELECT sum(ol_amount) as sumVal FROM orderline WHERE ol_w_id = %d "
            "AND ol_d_id = %d AND ol_o_id = %s",
            w_id_, d_id, order_id);
        LOG_INFO << GetSumQuery;
        const char* sumVal;
        auto sumResults = txn.exec(GetSumQuery);
        if (!sumResults.empty()) {
          sumVal = sumResults[0]["sumVal"].c_str();
        } else {
          sumVal = "0";
        }
        // get customer id
        std::string GetCustomerIdQuery = format(
            "SELECT o_c_id FROM orders WHERE o_w_id = %d AND o_d_id = %d AND "
            "o_id = %s",
            w_id_, d_id, order_id);
        LOG_INFO << GetCustomerIdQuery;
        auto cusResults = txn.exec(GetCustomerIdQuery);
        if (cusResults.empty()) {
          throw std::runtime_error("delivery: customer not found");
        }
        auto c_id = cusResults[0]["o_c_id"].as<int>();

        std::string UpdateCustomer = format(
            "UPDATE customer SET c_balance = c_balance + %s, c_delivery_cnt = "
            "c_delivery_cnt + 1 WHERE c_w_id = %d AND c_d_id = %d AND c_id = "
            "%d",
            sumVal, w_id_, d_id, c_id);
        LOG_INFO << UpdateCustomer;
        txn.exec(UpdateCustomer);
        txn.commit();
        break;
      } catch (const std::exception& e) {
        retryCount++;
        LOG_ERROR << e.what();
        LOG_INFO << "Retry time:" << retryCount;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(100 * retryCount));
      }
    }
    if (retryCount == MAX_RETRY_COUNT) {
      return Status::Invalid("retry times exceeded max retry count");
    }
  }
  time(&end_t);
  *diff_t = difftime(end_t, start_t);
  return Status::OK();
}
};  // namespace ydb_util