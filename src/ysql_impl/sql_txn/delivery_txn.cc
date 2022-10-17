#include "ysql_impl//sql_txn/delivery_txn.h"

#include <pqxx/pqxx>

#include "common/util/string_util.h"

namespace ydb_util {
Status YSQLDeliveryTxn::Execute() noexcept {
  LOG_INFO << "Delivery Transaction started";
  for (int d_id=0;d_id<=10;d_id++){
    int retryCount = 0;
    while (retryCount < MAX_RETRY_COUNT) {
      try {


        throw;
      }catch (const std::exception& e) {
        retryCount++;
        LOG_ERROR << e.what();
        LOG_INFO << "Retry time:"<<retryCount;
        std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
      }

    }
  }
  return Status::OK();
}
};