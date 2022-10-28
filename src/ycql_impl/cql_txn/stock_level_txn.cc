#include "ycql_impl/cql_txn/stock_level_txn.h"

#include <unordered_set>

#include "ycql_impl/cql_exe_util.h"

namespace ycql_impl {
using Status = ydb_util::Status;

Status YCQLStockLevelTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Stock-level Transaction started";
  auto st = Retry(std::bind(&YCQLStockLevelTxn::executeLocal, this),
                  MAX_RETRY_ATTEMPTS);
  if (st.ok()) LOG_INFO << "Stock-level transaction completed";
  return st;
}

Status YCQLStockLevelTxn::executeLocal() noexcept {
  Status st = Status::OK();

  CassIterator* next_order_it = nullptr;
  std::tie(st, next_order_it) = getNextOrder();
  if (!st.ok()) return st;
  auto next_o_id = GetValueFromCassRow<int32_t>(next_order_it, "d_next_o_id");
  if (next_order_it) cass_iterator_free(next_order_it);

  CassIterator* item_it = nullptr;
  std::tie(st, item_it) = getItemsInLastOrders(next_o_id);
  if (!st.ok()) return st;

  int items_below_threshold = 0;
  std::unordered_set<int32_t> i_id_set;
  while (cass_iterator_next(item_it)) {
    auto i_id = GetValueFromCassRow<int32_t>(item_it, "ol_i_id");
    if (i_id_set.find(i_id) != i_id_set.end()) continue;
    i_id_set.insert(i_id);

    CassIterator* quantity_it = nullptr;
    std::tie(st, quantity_it) = getItemQuantityFromStock(i_id);
    if (!st.ok()) return st;
    auto quantity = GetValueFromCassRow<int32_t>(quantity_it, "s_quantity");
    if (quantity < t_) ++items_below_threshold;

    if (quantity_it) cass_iterator_free(quantity_it);
  }
  std::cout << "Total number of items in S where its stock quantity at W_ID is "
               "below the threshold: "
            << items_below_threshold << std::endl;

  if (item_it) cass_iterator_free(item_it);
  return st;
}

std::pair<Status, CassIterator*> YCQLStockLevelTxn::getNextOrder() noexcept {
  std::string stmt =
      "SELECT d_next_o_id "
      "FROM " +
      YCQLKeyspace +
      ".district "
      "WHERE d_w_id = ? AND d_id = ? "
      ";";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_);
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("NextOrder: District not found"), it};
  }
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLStockLevelTxn::getItemsInLastOrders(
    int32_t next_o_id) noexcept {
  std::string stmt =
      "SELECT ol_i_id "
      "FROM " +
      YCQLKeyspace +
      ".orderline "
      "WHERE ol_w_id = ? AND ol_d_id = ? "
      "AND ol_o_id >= ? AND ol_o_id < ? "
      ";";
  LOG_INFO << stmt;
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_,
                                        next_o_id - l_, next_o_id);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLStockLevelTxn::getItemQuantityFromStock(
    int32_t i_id) noexcept {
  std::string stmt =
      "SELECT s_quantity "
      "FROM " +
      YCQLKeyspace +
      ".stock "
      "WHERE s_w_id = ? AND s_i_id = ? "
      ";";
  CassIterator* it = nullptr;
  LOG_INFO << stmt;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, i_id);
  if (!cass_iterator_next(it)) {
    return {Status::ExecutionFailed("Item quantity not found"), it};
  }
  cass_iterator_next(it);
  return {st, it};
}

}  // namespace ycql_impl
