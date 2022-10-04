#ifndef YDB_PERF_PARSER_H_
#define YDB_PERF_PARSER_H_

#include <iostream>
#include <string>

#include "common/txn/delivery_txn.h"
#include "common/txn/new_order_txn.h"
#include "common/txn/order_status_txn.h"
#include "common/txn/payment_txn.h"
#include "common/txn/popular_item_txn.h"
#include "common/txn/related_customer_txn.h"
#include "common/txn/stock_level_txn.h"
#include "common/txn/top_balance_txn.h"
#include "common/util/status.h"

namespace ydb_util {

// Usage: After init the Parser class with input file path
// Call GetNextTxn to get next Txn class.
template <typename Connection>
class Parser {
  using Txn = Txn<Connection>;

 public:
  explicit Parser(const std::string& file_name, Connection* conn)
      : file_name_(file_name), conn_(conn) {}

  Status Init() noexcept {
    fs_ = std::ifstream(file_name_, std::ios::in);
    if (!fs_.is_open()) {
      return Status::IOError("Fail to open " + file_name_);
    }
    return Status::OK();
  }

  Status GetNextTxn(Txn** txn) noexcept {
    assert(txn != nullptr);
    if (!fs_.good()) {
      return Status::EndOfFile();
    }
    std::string line;
    fs_ >> line;
    assert(!line.empty());
    auto txn_ptr = GetTxnPtr_(line[0]);
    auto ret = txn_ptr->Init(line, fs_);
    *txn = txn_ptr;
    return ret;
  }

 private:
  Txn* GetTxnPtr_(char c) noexcept {
    Txn* txn = nullptr;
    switch (c) {
      case 'N': {
        // New Order
        txn = new NewOrderTxn(conn_);
        break;
      }
      case 'P': {
        // Payment
        // Unimplemented yet
        // txn = new PaymentTxn(conn_);
        break;
      }
      case 'D': {
        // Delivery
        // Unimplemented yet
        // txn = new DeliveryTxn(conn_);
        break;
      }
      case 'O': {
        // Order-Status
        // Unimplemented yet
        // txn = new OrderStatusTxn(conn_);
        break;
      }
      case 'S': {
        // Stock-Level
        // Unimplemented yet
        // txn = new StockLevelTxn(conn_);
        break;
      }
      case 'I': {
        // Popular-Item
        // Unimplemented yet
        // txn = new PopularItemTxn(conn_);
        break;
      }
      case 'T': {
        // Top-Balance
        // Unimplemented yet
        // txn = new TopBalanceTxn(conn_);
        break;
      }
      case 'R': {
        // Related_Customer
        // Unimplemented yet
        // txn = new RelatedCustomerTxn(conn_);
        break;
      }
      default: {
        // Unreachable
        assert(false);
      }
    }
    return txn;
  }

  std::string file_name_;
  std::ifstream fs_;
  Connection* conn_;
};
}  // namespace ydb_util

#endif