#include "ysql_impl//sql_txn/delivery_txn.h"

#include <pqxx/pqxx>

#include "common/util/string_util.h"

namespace ydb_util {
Status YSQLDeliveryTxn::Execute() noexcept {
  LOG_INFO << "Delivery Transaction started";
  for (int d_id=1;d_id<=10;d_id++){
    int retryCount = 0;
    while (retryCount < MAX_RETRY_COUNT) {
      try {
        pqxx::work txn(*conn_);
        LOG_INFO << ">>>> Get Order:";
        std::string OrderQuery = format(
            "SELECT MIN(O_ID) as O_ID FROM orders WHERE O_W_ID = %d AND O_D_ID = %d AND O_CARRIER_ID is NULL",
            w_id_,d_id);
        pqxx::result orders =txn.exec(OrderQuery);
        if (orders.empty()){
          throw std::runtime_error("delivery: order not found");
        }
        auto order = orders[0];
        std::string UpdateOrder = format("UPDATE orders SET o_carrier_id = %d WHERE o_w_id = %d AND o_d_id = %d AND o_id = %s",
                                         carrier_id_,w_id_,d_id,order["O_ID"].c_str());
        txn.exec(UpdateOrder);
//        std::string UpdateOrderLine =

        txn.commit();
        break;
      }catch (const std::exception& e) {
        retryCount++;
        LOG_ERROR << e.what();
        LOG_INFO << "Retry time:"<<retryCount;
        std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
      }
    }
    if (retryCount == MAX_RETRY_COUNT){
      return Status::Invalid("retry times exceeded max retry count");
    }
  }
  return Status::OK();
}
};