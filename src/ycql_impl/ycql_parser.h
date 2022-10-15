#ifndef YCQL_IMPL_PARSER_H_
#define YCQL_IMPL_PARSER_H_
#include "cassandra.h"
#include "common/parser/parser.h"
#include "ycql_impl/cql_txn/delivery_txn.h"
#include "ycql_impl/cql_txn/new_order_txn.h"
#include "ycql_impl/cql_txn/order_status_txn.h"
#include "ycql_impl/cql_txn/payment_txn.h"
#include "ycql_impl/cql_txn/popular_item_txn.h"
#include "ycql_impl/cql_txn/related_customer_txn.h"
#include "ycql_impl/cql_txn/stock_level_txn.h"
#include "ycql_impl/cql_txn/top_balance_txn.h"

namespace ydb_util {
class YCQLParser : public Parser {
 public:
  YCQLParser(const std::string& file_name, CassSession* session)
      : Parser(file_name), conn_(session) {}

 protected:
  Txn* GetTxnPtr_(char c) noexcept override {
    Txn* txn = nullptr;
    switch (c) {
      case 'N': {
        // New Order
        txn = new YCQLNewOrderTxn(conn_);
        break;
      }
      case 'P': {
        // Payment
        // Unimplemented yet
        LOG_INFO << "Payment";
        txn = new YCQLPaymentTxn(conn_);
        break;
      }
      case 'D': {
        // Delivery
        txn = new YCQLDeliveryTxn(conn_);
        break;
      }
      case 'O': {
        // Order-Status
        txn = new YCQLOrderStatusTxn(conn_);
        break;
      }
      case 'S': {
        // Stock-Level
        txn = new YCQLStockLevelTxn(conn_);
        break;
      }
      case 'I': {
        // Popular-Item
        txn = new YCQLPopularItemTxn(conn_);
        break;
      }
      case 'T': {
        // Top-Balance
        txn = new YCQLTopBalanceTxn(conn_);
        break;
      }
      case 'R': {
        // Related-Customer
        txn = new YCQLRelatedCustomerTxn(conn_);
        break;
      }
      default: {
        // Unreachable
        assert(false);
      }
    }
    return txn;
  }

 private:
  CassSession* conn_;
};
}  // namespace ydb_util
#endif