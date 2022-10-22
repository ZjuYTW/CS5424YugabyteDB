#include "ycql_impl/cql_txn/stock_level_txn.h"
#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;
Status YCQLStockLevelTxn::Execute() noexcept {
  LOG_INFO << "Stock-level Transaction started";
  int retry_time = 0;
  Status st;
  CassIterator *item_it = nullptr;
  bool done = false;

  do {
    auto next_o_id = getNextOrderId();
    if (!item_it) {
      std::tie(st, item_it) = getItemsInLastOrders(next_o_id);
      if (!st.ok()) continue;
    }
    int items_below_threshold = 0;
    while (cass_iterator_next(item_it)) {
      auto i_id = GetValueFromCassRow<uint32_t>(item_it, "ol_i_id");
      auto quantity = GetValueFromCassRow<uint32_t>(getItemQuantityFromStock(i_id), "s_quantity");
      if (quantity < t_) ++items_below_threshold;
    }
    std::cout << "Total number of items in S where its stock quantity at W_ID is below the threshold: "
              << items_below_threshold << std::endl;

  } while (retry_time++ < MaxRetryTime /* && sleep(done) */);
  if (item_it) cass_iterator_free(item_it);
  return st;
}


uint32_t YCQLStockLevelTxn::getNextOrderId() {
  std::string stmt =
      "SELECT d_next_o_id FROM district "
      "WHERE d_w_id = ? AND d_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  // TODO(winston.yan): handle error
  auto _ = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_);

  if (!cass_iterator_next(it)) {
    throw std::string("District not found");
  }
  return GetValueFromCassRow<uint32_t>(it, "d_next_o_id");
}

std::pair<Status, CassIterator*> YCQLStockLevelTxn::getItemsInLastOrders(uint32_t next_o_id) {
  std::string stmt =
      "SELECT DISTINCT ol_i_id "
      "FROM orderline "
      "WHERE ol_w_id = ? AND ol_d_id = ? "
      "AND ol_o_id >= ? AND ol_o_id < ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_, next_o_id - l_, next_o_id);
  return {st, it};
}

CassIterator *YCQLStockLevelTxn::getItemQuantityFromStock(uint32_t i_id) {
  std::string stmt =
      "SELECT s_quantity "
      "FROM stock "
      "WHERE s_w_id = ? AND s_i_id = ? "
      "ALLOW FILTERING;";
  CassIterator* it = nullptr;
  // TODO(winston.yan): handle error
  auto _ = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, i_id);

  if (!cass_iterator_next(it)) {
    throw std::string("Item quantity not found");
  }
  return it;
}


}
