#include "ycql_impl/cql_txn/new_order_txn.h"

#include <chrono>
#include <future>

#include "cassandra.h"
#include "common/util/logger.h"
#include "common/util/string_util.h"
#include "ycql_impl/cql_exe_util.h"
#include "ycql_impl/cql_txn/stock_level_txn.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {
using Status = ydb_util::Status;

Status YCQLNewOrderTxn::Execute(double* diff_t) noexcept {
  if (YDB_SKIP_NEW_ORDER) {
    *diff_t = 0;
    return Status::OK();
  }
#ifndef NDEBUG
  if (trace_timer_) {
    trace_timer_->Reset();
  }
#endif
  LOG_INFO << "New-Order Transaction started";
  std::vector<OrderLine> order_lines;
  order_lines.reserve(orders_.size());
  int all_local = 1;

  for (auto& str : orders_) {
    int32_t i_id, w_id, quantity;
    auto st = ParseOneOrder(str, &i_id, &w_id, &quantity);
    assert(st.ok());
    if (w_id != w_id_) {
      all_local = 0;
    }
    order_lines.push_back(
        OrderLine{.i_id = i_id, .w_id = w_id, .quantity = quantity});
  }

  auto NewOrder = ydb_util::format("N %d %d %d", w_id_, d_id_, c_id_);
  // 1. customer identifier 2. tax rates 3. Order Infos 4. Items Info 5. n lines
  // for each order item info
  outputs_.resize(4 + order_lines.size());
  auto start_time = std::chrono::system_clock::now();
  auto st = Retry(
      std::bind(&YCQLNewOrderTxn::executeLocal, this, order_lines, all_local),
      1);
  auto end_time = std::chrono::system_clock::now();
  *diff_t = (end_time - start_time).count();
  if (st.ok()) {
    LOG_INFO << "New-Order Transaction end, time cost" << *diff_t;
    txn_out_ << NewOrder << std::endl;
    for (auto& str : orders_) {
      txn_out_ << str << std::endl;
    }
    for (auto& str : outputs_) {
      txn_out_ << "\t" << str << std::endl;
    }
  } else {
    err_out_ << st.ToString() << std::endl;
    err_out_ << NewOrder << std::endl;
    for (auto& str : orders_) {
      err_out_ << str << std::endl;
    }
  }
#ifndef NDEBUG
  if (trace_timer_) trace_timer_->Print();
#endif
  return st;
}

Status YCQLNewOrderTxn::executeLocal(std::vector<OrderLine>& order_lines,
                                     int all_local) noexcept {
  Status st;
  CassIterator *district_it = nullptr, *custom_it = nullptr,
               *warehouse_it = nullptr;
  const CassResult *district_result = nullptr, *custom_result = nullptr,
                   *warehouse_result = nullptr;
  auto free_func = [&district_it, &custom_it, &warehouse_it, &district_result,
                    &custom_result, &warehouse_result]() {
    if (district_it) {
      cass_iterator_free(district_it);
    }
    if (custom_it) {
      cass_iterator_free(custom_it);
    }
    if (warehouse_it) {
      cass_iterator_free(warehouse_it);
    }
    if (district_result) {
      cass_result_free(district_result);
    }
    if (custom_result) {
      cass_result_free(custom_result);
    }
    if (warehouse_result) {
      cass_result_free(warehouse_result);
    }
  };
  DEFER(std::move(free_func));

  std::tie(st, district_it) = getDistrict(&district_result);
  if (!st.ok()) {
    return st;
  }
  std::tie(st, custom_it) = getCustomer(&custom_result);
  if (!st.ok()) {
    return st;
  }
  std::tie(st, warehouse_it) = getWarehouse(&warehouse_result);
  if (!st.ok()) {
    return st;
  }
  uint32_t next_o_id = -1;
  {
    // To get correct NextOID, here we need to handle the race condition in
    // CQL
    next_o_id =
        ycql_impl::GetValueFromCassRow<int32_t>(district_it, "d_next_o_id")
            .value();
    auto origin_o_id = next_o_id;
    bool succ = true;
    do {
      succ = updateNextOId(++next_o_id, origin_o_id);
    } while (!succ);
    next_o_id--;
  }
  st = processOrderNonDelivery(next_o_id);
  if (!st.ok()) {
    return st;
  }
  auto [s, total_amount] = processOrderLines(order_lines, next_o_id);
  if (!s.ok()) {
    return s;
  }
  st = processOrderMaxQuantity(order_lines, next_o_id, total_amount);
  if (!st.ok()) {
    return st;
  }
  auto d_tax = GetDTax(district_it);
  auto w_tax = GetWTax(warehouse_it);
  auto c_discount = GetDiscount(custom_it);
  total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount);
  std::string current_time = ydb_util::getLocalTimeString();
  st = processOrder(next_o_id, order_lines.size(), all_local, current_time);
  assert(st.ok());
  processOutput(custom_it, total_amount, current_time, c_discount, w_tax, d_tax,
                next_o_id);
  return st;
}

Status YCQLNewOrderTxn::processOrder(int32_t next_o_id, int32_t order_num,
                                     int all_local,
                                     std::string& current_time) noexcept {
  TRACE_GUARD
  std::string stmt =
      "INSERT INTO " + YCQLKeyspace +
      ".orders(o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt, o_all_local, "
      "o_entry_d)"
      "VALUES(?, ?, ?, ?, ?, ?, '" +
      current_time.substr(0, current_time.size() - 2) + "')";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id_, next_o_id,
                                      c_id_, order_num, all_local);
}

Status YCQLNewOrderTxn::processOrderMaxQuantity(
    const std::vector<OrderLine>& order_lines, int32_t next_o_id,
    int64_t total_amount) noexcept {
  TRACE_GUARD
  std::vector<std::pair<int32_t, int32_t>> sort_orders;
  sort_orders.reserve(order_lines.size());
  for (auto& ol : order_lines) {
    sort_orders.push_back({ol.quantity, ol.i_id});
  }
  std::sort(sort_orders.begin(), sort_orders.end(), [](auto& lhs, auto& rhs) {
    return lhs.first == rhs.first ? lhs.second > rhs.second
                                  : lhs.first > rhs.first;
  });
  auto max_quantity = sort_orders[0].first;
  std::vector<int32_t> item_ids;
  for (auto& ol : sort_orders) {
    if (ol.first != max_quantity) break;
    item_ids.push_back(ol.second);
  }
  std::string stmt = "INSERT INTO " + YCQLKeyspace +
                     ".order_max_quantity(o_w_id, o_d_id, o_id, max_quantity, "
                     "item_ids, total_amount) VALUES (?, ?, ?, ?, ?, ?)";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id_, next_o_id,
                                      max_quantity, item_ids, total_amount);
}

Status YCQLNewOrderTxn::processOrderNonDelivery(int32_t next_o_id) noexcept {
  TRACE_GUARD
  Status s;
  std::string stmt =
      "INSERT INTO " + YCQLKeyspace +
      ".order_non_delivery(o_w_id, o_d_id, o_id, o_c_id) VALUES(?, ?, ?, ?)";
  return ycql_impl::execute_write_cql(conn_, stmt, w_id_, d_id_, next_o_id,
                                      c_id_);
}

// TODO(ZjuYTW): Refactor the following shit
std::pair<Status, int64_t> YCQLNewOrderTxn::processOrderLines(
    std::vector<OrderLine>& order_lines, int32_t next_o_id) noexcept {
  TRACE_GUARD
  std::atomic<int64_t> total_amount = 0;
  Status s;

  std::vector<std::future<Status>> fts;
  fts.reserve(order_lines.size());
  for (size_t i = 0; i < order_lines.size(); i++) {
    auto exec_one_order_line = [this, &order_lines, &total_amount,
                                &next_o_id](size_t i) {
      if (i == 0) {
        TRACE_GUARD
      }
      CassIterator *stock_it = nullptr, *item_it = nullptr;
      const CassResult *stock_result = nullptr, *item_result = nullptr;
      Status s;
      auto free_func = [&stock_it, &item_it, &stock_result, &item_result]() {
        if (stock_it) {
          cass_iterator_free(stock_it);
        }
        if (item_it) {
          cass_iterator_free(item_it);
        }
        if (stock_result) {
          cass_result_free(stock_result);
        }
        if (item_result) {
          cass_result_free(item_result);
        }
      };
      DEFER(std::move(free_func));
      std::tie(s, stock_it) =
          getStock(order_lines[i].i_id, order_lines[i].w_id, &stock_result);
      if (!s.ok()) {
        LOG_DEBUG << "Get Stock failed, " << s.ToString();
        return s;
      }
      auto stock_quantity =
          ycql_impl::GetValueFromCassRow<int32_t>(stock_it, "s_quantity")
              .value();
      std::string dist_col =
          "s_dist_" + ((d_id_ < 10 ? "0" : "") + std::to_string(d_id_));
      auto stock_info = ycql_impl::GetValueFromCassRow<std::string>(
          stock_it, dist_col.c_str());
      int32_t adjusted_qty = stock_quantity - order_lines[i].quantity;
      if (adjusted_qty < 10) {
        adjusted_qty += 100;
      }
      s = updateStock(adjusted_qty, stock_quantity, order_lines[i].quantity,
                      (order_lines[i].w_id != w_id_), order_lines[i].w_id,
                      order_lines[i].i_id);
      std::tie(s, item_it) = getItem(order_lines[i].i_id, &item_result);
      if (!s.ok()) {
        // Maybe timeout?
        LOG_DEBUG << "Update Stock failed, " << s.ToString();
        return s;
      }
      auto i_price =
          ycql_impl::GetValueFromCassRow<int32_t>(item_it, "i_price").value();

      auto i_name =
          ycql_impl::GetValueFromCassRow<std::string>(item_it, "i_name")
              .value();
      // Note: this item_amount is enlarged by 100, when output we need to
      // convert back
      int64_t item_amount = order_lines[i].quantity * i_price;
      // create one order-line
      std::string stmt =
          "INSERT INTO " + YCQLKeyspace +
          ".orderline (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, "
          "ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)"
          "VALUES(?,?,?,?,?,?,?,?,?)";
      s = ycql_impl::execute_write_cql(
          conn_, stmt, w_id_, d_id_, next_o_id, (int32_t)i + 1,
          order_lines[i].i_id, item_amount, order_lines[i].w_id,
          order_lines[i].quantity, dist_col.c_str());
      if (!s.ok()) {
        // Timeout?
        LOG_DEBUG << "Create One Order-Line failed, " << s.ToString();
        return s;
      }
      processItemOutput(i, order_lines[i], item_amount, adjusted_qty, i_name);
      total_amount.fetch_add(item_amount);
      return s;
    };
    fts.push_back(std::async(std::launch::async, exec_one_order_line, i));
  }
  for (auto& ft : fts) {
    if (!ft.get().ok()) {
      return {ft.get(), -1};
    }
  }
  return {Status::OK(), total_amount.load()};
}

bool YCQLNewOrderTxn::updateNextOId(int32_t next_o_id,
                                      int32_t prev_next_o_id) noexcept {
  TRACE_GUARD
  std::string stmt =
      "UPDATE " + YCQLKeyspace +
      ".district SET d_next_o_id = ? WHERE d_w_id = ? and d_id = ? IF "
      "d_next_o_id = ?";
  CassStatement* statement = cass_statement_new(stmt.c_str(), 4);
  auto rc = ycql_impl::cql_statement_fill_args(statement, next_o_id, w_id_, d_id_, prev_next_o_id);
  auto future = cass_session_execute(conn_, statement);
  auto result = cass_future_get_result(future);
  if (result == nullptr) {
    cass_statement_free(statement);
    cass_future_free(future);
    return false; 
  }
  cass_bool_t appiled;
  auto value = cass_row_get_column(cass_result_first_row(result), 0);
  cass_value_get_bool(value, &appiled);
  cass_statement_free(statement);
  cass_future_free(future);
  cass_result_free(result);
  return value;
}

Status YCQLNewOrderTxn::updateStock(int32_t adjusted_qty, int32_t prev_qty,
                                    int32_t ordered_qty, int remote_cnt,
                                    int32_t w_id, int32_t item_id) noexcept {
  std::string stmt = "UPDATE " + YCQLKeyspace +
                     ".stock SET s_quantity = ?, s_ytd = s_ytd + " +
                     std::to_string(ordered_qty) +
                     ", s_order_cnt = "
                     "s_order_cnt + 1, s_remote_cnt = s_remote_cnt + " +
                     std::to_string(remote_cnt) +
                     " WHERE s_w_id = ? and "
                     "s_i_id = ? IF s_quantity = ?";
  return ycql_impl::execute_write_cql(conn_, stmt, adjusted_qty, w_id, item_id,
                                      prev_qty);
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getItem(
    int32_t item_id, const CassResult** result) noexcept {
  std::string stmt = "SELECT * FROM " + YCQLKeyspace + ".item WHERE i_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, item_id);
  if (!st.ok()) {
    LOG_DEBUG << "getItem failed, " + st.ToString();
    return {st, it};
  }
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getDistrict(
    const CassResult** result) noexcept {
  TRACE_GUARD
  std::string stmt = "SELECT d_tax, d_next_o_id FROM " + YCQLKeyspace +
                     ".district WHERE d_w_id = ? and d_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id_, d_id_);
  if (!st.ok()) {
    LOG_DEBUG << "getDistrict failed, " + st.ToString();
    return {st, it};
  }
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getWarehouse(
    const CassResult** result) noexcept {
  TRACE_GUARD
  std::string stmt =
      "SELECT w_tax FROM " + YCQLKeyspace + ".warehouse WHERE w_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id_);
  if (!st.ok()) {
    LOG_DEBUG << "getWarehouse failed, " + st.ToString();
    return {st, it};
  }
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getCustomer(
    const CassResult** result) noexcept {
  TRACE_GUARD
  std::string stmt = "SELECT c_last, c_credit, c_discount FROM " +
                     YCQLKeyspace +
                     ".customer WHERE c_w_id = ? and c_d_id = ? and c_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id_, d_id_,
                                        c_id_);
  if (!st.ok()) {
    LOG_DEBUG << "getCustomer failed, " + st.ToString();
    return {st, it};
  }
  cass_iterator_next(it);
  return {st, it};
}

std::pair<Status, CassIterator*> YCQLNewOrderTxn::getStock(
    int32_t i_id, int32_t w_id, const CassResult** result) noexcept {
  std::string stmt = "SELECT s_quantity, s_dist_" +
                     ((d_id_ < 10 ? "0" : "") + std::to_string(d_id_)) +
                     " FROM " + YCQLKeyspace +
                     ".stock WHERE s_w_id = ? and s_i_id = ?";
  CassIterator* it = nullptr;
  auto st = ycql_impl::execute_read_cql(conn_, stmt, result, &it, w_id, i_id);
  if (!st.ok()) {
    LOG_DEBUG << "getStock failed, " + st.ToString();
    return {st, it};
  }
  cass_iterator_next(it);
  return {st, it};
}

void YCQLNewOrderTxn::processItemOutput(size_t start_idx, const OrderLine& ol,
                                        int64_t item_amount, int32_t s_quantity,
                                        const std::string& i_name) noexcept {
  assert(start_idx + 4 < outputs_.size());
  outputs_[start_idx + 4] = ydb_util::format(
      "ITEM_NUMBER: %d, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, "
      "OL_AMOUNT: %.2f, S_QUANTITY: %d",
      ol.i_id, i_name.c_str(), ol.w_id, ol.quantity, (double)item_amount / 100,
      s_quantity);
}

void YCQLNewOrderTxn::processOutput(CassIterator* customer_it,
                                    int64_t total_amount,
                                    std::string& current_time, double discount,
                                    double w_tax, double d_tax,
                                    int32_t o_id) noexcept {
  // Customer Info
  auto c_credit = GetValueFromCassRow<std::string>(customer_it, "c_credit");
  auto c_last = GetValueFromCassRow<std::string>(customer_it, "c_last");
  outputs_[0] = ydb_util::format(
      "Customer identifier (W_ID: %d, D_ID: %d, C_ID: %d), C_LAST: %s, "
      "C_CREDIT: %s, C_DISCOUNT: %.4f",
      w_id_, d_id_, c_id_, c_last->c_str(), c_credit->c_str(), discount);
  // Warehouse Info & District Info
  outputs_[1] = ydb_util::format("W_TAX: %.4f, D_TAX: %.4f", w_tax, d_tax);
  // Order Info
  outputs_[2] =
      ydb_util::format("O_ID: %d, O_ENTRY_D: %s", o_id, current_time.c_str());
  // number Info
  outputs_[3] =
      ydb_util::format("NUM_ITEMS: %d, TOTAL_AMOUNT: %.2f",
                       this->orders_.size(), (double)total_amount / 100);
}
}  // namespace ycql_impl
