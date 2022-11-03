#include "ycql_impl/cql_txn/stock_level_txn.h"

#include <unordered_set>

#include "ycql_impl/cql_exe_util.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {
using Status = ydb_util::Status;
using ydb_util::format;

Status YCQLStockLevelTxn::Execute(double* diff_t) noexcept {
  if (YDB_SKIP_STOCK_LEVEL) {
    *diff_t = 0;
    return Status::OK();
  }
#ifndef NDEBUG
  if (trace_timer_) {
    trace_timer_->Reset();
  }
#endif
  LOG_INFO << "Stock-level Transaction started";
  const auto InputString =
      format("S %d %d %d %d", this->w_id_, this->d_id_, this->l_, this->t_);
  outputs_.reserve(1);
  auto start_time = std::chrono::system_clock::now();
  auto st = Retry(std::bind(&YCQLStockLevelTxn::executeLocal, this),
                  MAX_RETRY_ATTEMPTS);
  auto end_time = std::chrono::system_clock::now();
  *diff_t = (end_time - start_time).count();
  if (st.ok()) {
    LOG_INFO << "Stock-level transaction completed, time cost " << *diff_t;
    // Txn output
    txn_out_ << InputString << std::endl;
    for (const auto& ostr : outputs_) {
      txn_out_ << "\t" << ostr << std::endl;
    }
  } else {
    err_out_ << InputString << std::endl;
    err_out_ << st.ToString() << std::endl;
  }
  return st;
}

Status YCQLStockLevelTxn::executeLocal() noexcept {
  Status st = Status::OK();

  CassIterator* next_order_it = nullptr;
  std::tie(st, next_order_it) = getNextOrder();
  if (!st.ok()) return st;
  auto next_o_id =
      GetValueFromCassRow<int32_t>(next_order_it, "d_next_o_id").value();
  if (next_order_it) cass_iterator_free(next_order_it);

  CassIterator* item_it = nullptr;
  std::tie(st, item_it) = getItemsInLastOrders(next_o_id);
  if (!st.ok()) return st;

  int items_below_threshold = 0;
  std::unordered_set<int32_t> i_id_set;
  while (cass_iterator_next(item_it)) {
    auto i_id = GetValueFromCassRow<int32_t>(item_it, "ol_i_id").value();
    if (i_id_set.find(i_id) != i_id_set.end()) continue;
    i_id_set.insert(i_id);

    CassIterator* quantity_it = nullptr;
    std::tie(st, quantity_it) = getItemQuantityFromStock(i_id);
    if (!st.ok()) return st;
    auto quantity = GetValueFromCassRow<int32_t>(quantity_it, "s_quantity");
    if (quantity.value() < t_) ++items_below_threshold;
    LOG_INFO << "i_id: " << i_id << ", quantity: " << quantity.value();
    if (quantity_it) cass_iterator_free(quantity_it);
  }

  outputs_.push_back(
      format("Total number of items in S where its stock quantity at W_ID "
             "is below the threshold: %d",
             items_below_threshold));

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
  if (!st.ok()) return {st, it};
  if (!cass_iterator_next(it))
    return {Status::ExecutionFailed("NextOrder: District not found"), it};
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
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, i_id);
  if (!st.ok()) return {st, it};
  if (!cass_iterator_next(it))
    return {Status::ExecutionFailed("Item quantity not found"), it};
  return {st, it};
}

}  // namespace ycql_impl
