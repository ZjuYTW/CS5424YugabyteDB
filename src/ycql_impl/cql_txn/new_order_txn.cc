#include "ycql_impl/cql_txn/new_order_txn.h"

#include <chrono>

#include "cassandra.h"
#include "common/util/logger.h"
#include "ycql_impl/cql_exe_util.h"
#include "ycql_impl/cql_txn/stock_level_txn.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {
using Status = ydb_util::Status;
Status YCQLNewOrderTxn::Execute(double* diff_t) noexcept {
  std::vector<OrderLine> order_lines;
  order_lines.reserve(orders_.size());
  int all_local = 1;
  // Preprocess order lines
  for (auto& str : orders_) {
    uint32_t i_id, w_id, quantity;
    auto st = ParseOneOrder(str, &i_id, &w_id, &quantity);
    if (!st.ok()) {
      return st;
    }
    // If current orderlines's warehouse id != local warehouse id
    if (w_id != w_id_) {
      all_local = 0;
    }
    order_lines.push_back(
        OrderLine{.i_id = i_id, .w_id = w_id, .quantity = quantity});
  }

  int retry_time = 0;
  Status st;
  CassIterator *district_it = nullptr, *custom_it = nullptr,
               *warehouse_it = nullptr;
  bool done = false;
  do {
    if (!district_it) {
      LOG_DEBUG << "Get District";
      std::tie(st, district_it) = getDistrict();
      if (!st.ok()) continue;
    }
    if (!custom_it) {
      LOG_DEBUG << "Get Customer";
      std::tie(st, custom_it) = getCustomer();
      if (!st.ok()) continue;
    }
    if (!warehouse_it) {
      LOG_DEBUG << "Get Warehouse";
      std::tie(st, warehouse_it) = getWarehouse();
      if (!st.ok()) continue;
    }
    uint32_t next_o_id = -1;
    {
      // To get correct NextOID, here we need to handle the race condition in
      // CQL
      next_o_id =
          ycql_impl::GetValueFromCassRow<int32_t>(district_it, "d_next_o_id");
      auto origin_o_id = next_o_id;
      do {
        st = updateNextOId(++next_o_id, origin_o_id);
      } while (!st.ok());
    }

    auto [_, total_amount] = processOrderLines(order_lines, next_o_id);
    LOG_DEBUG << "Get d_tax";
    auto d_tax = GetDTax(district_it);
    auto w_tax = GetWTax(warehouse_it);
    auto c_discount = GetDiscount(custom_it);
    total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount);
    st = processOrder(next_o_id, order_lines.size(), all_local, total_amount);
    if (!st.ok()) {
      continue;
    }
    done = true;
  } while (retry_time++ < MaxRetryTime && !ValidOrSleep(done));
  if (district_it) {
    cass_iterator_free(district_it);
  }
  if (custom_it) {
    cass_iterator_free(custom_it);
  }
  if (warehouse_it) {
    cass_iterator_free(warehouse_it);
  }
  return st;
}

Status YCQLNewOrderTxn::processOrder(uint32_t next_o_id, uint32_t order_num,
                                     int all_local,
                                     int64_t total_amount) noexcept {
  std::string stmt =
      "INSERT INTO " + YCQLKeyspace + ".orders(o_w_id, o_d_id, o_id, o_c_id, o_cl_cnt, o_all_local, "
      "o_entry_d)"
      "VALUES(?, ?, ?, ?, ?, ?, currenttimestamp())";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id_, c_id_,
                                      order_num, all_local);
}

// TODO(ZjuYTW): Refactor the following shit
std::pair<Status, int64_t> YCQLNewOrderTxn::processOrderLines(
    std::vector<OrderLine>& order_lines, uint32_t next_o_id) noexcept {
  std::vector<int> succ(order_lines.size(), 0);

  int64_t total_amount = 0;

  for (size_t i = 0; i < order_lines.size(); i++) {
    if (succ[i]) continue;
    auto [s, stock] = getStock(order_lines[i].i_id, order_lines[i].w_id);
    if (!s.ok()) continue;
    LOG_DEBUG << "Get StockQuantity";
    auto stock_quantity =
        ycql_impl::GetValueFromCassRow<int32_t>(stock, "s_quantity");
    std::string dist_col = "s_dist_" + ((d_id_ < 10 ? "0" : "") + std::to_string(d_id_));
    LOG_DEBUG << "Get StockInfo";
    auto stock_info =
        ycql_impl::GetValueFromCassRow<std::string>(stock, dist_col.c_str());
    uint32_t adjusted_qty = stock_quantity - order_lines[i].quantity;
    if (adjusted_qty < 10) {
      adjusted_qty += 100;
    }
    LOG_DEBUG << "Update Stock";
    s = updateStock(adjusted_qty, stock_quantity, order_lines[i].quantity,
                    (order_lines[i].w_id != w_id_), order_lines[i].w_id,
                    order_lines[i].i_id);
    LOG_DEBUG << "Get Item";
    auto [st, item] = getItem(order_lines[i].i_id);
    if (!st.ok()) {
      // Maybe timeout?
      continue;
    }
    LOG_DEBUG << "Get Item Price";
    auto i_price = ycql_impl::GetValueFromCassRow<int32_t>(item, "i_price");
    int32_t item_amount = order_lines[i].quantity * i_price;
    // create one order-line
    std::string stmt =
        "INSERT " + YCQLKeyspace + ".orderline (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, "
        "ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)"
        "VALUES(?,?,?,?,?,?,?,?,?)";
    LOG_DEBUG << "Insert OrderLines";
    s = ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id_, next_o_id,
                                     (int32_t)i + 1, order_lines[i].i_id,
                                     item_amount, order_lines[i].w_id,
                                     order_lines[i].quantity, dist_col.c_str());
    if (!s.ok()) {
      // Timeout?
      continue;
    }
    total_amount += item_amount;
    succ[i] = 1;
  }
  return {Status::OK(), total_amount};
}

Status YCQLNewOrderTxn::updateNextOId(uint32_t next_o_id,
                                      uint32_t prev_next_o_id) noexcept {
  std::string stmt =
      "UPDATE " + YCQLKeyspace + ".district SET d_next_o_id = ? WHERE d_w_id = ? and d_id = ? IF "
      "d_next_o_id = ?";
  return ycql_impl::execute_write_cql(conn_, stmt, next_o_id, w_id_, d_id_,
                                      prev_next_o_id);
}

Status YCQLNewOrderTxn::updateStock(uint32_t adjusted_qty, uint32_t prev_qty,
                                    uint32_t ordered_qty, int remote_cnt,
                                    uint32_t w_id, uint32_t item_id) noexcept {
  std::string stmt =
      "UPDATE " + YCQLKeyspace + ".stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = "
      "s_order_cnt + 1, s_remote_cnt = s_remote_cnt + ? WHERE s_w_id = ? and "
      "s_i_id = ? IF s_quantity = ?";
  return ycql_impl::execute_write_cql(conn_, stmt, adjusted_qty, ordered_qty,
                                      remote_cnt, w_id, item_id, prev_qty);
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getItem(
    uint32_t item_id) noexcept {
  std::string stmt = "SELECT * FROM " + YCQLKeyspace + ".item WHERE i_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, item_id);
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getDistrict() noexcept {
  std::string stmt = "SELECT * FROM " + YCQLKeyspace + ".district WHERE d_w_id = ? and d_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_);
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getWarehouse() noexcept {
  std::string stmt = "SELECT * FROM " + YCQLKeyspace + ".warehouse WHERE w_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_);
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getCustomer() noexcept {
  std::string stmt =
      "SELECT * FROM " + YCQLKeyspace + ".customer WHERE c_w_id = ? and c_d_id = ? and c_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id_, d_id_, c_id_);
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getStock(
    uint32_t i_id, uint32_t w_id) noexcept {
  std::string stmt = "SELECT * FROM " + YCQLKeyspace + ".stock WHERE s_w_id = ? and s_i_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, &it, w_id, i_id);
  cass_iterator_next(it);
  return {st, it};
}
}  // namespace ycql_impl
