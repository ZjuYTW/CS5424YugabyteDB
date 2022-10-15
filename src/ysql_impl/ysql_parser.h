#ifndef YSQL_IMPL_PARSER_H_
#define YSQL_IMPL_PARSER_H_
#include <pqxx/pqxx>

#include "common/parser/parser.h"
#include "ysql_impl/sql_txn/delivery_txn.h"
#include "ysql_impl/sql_txn/new_order_txn.h"
#include "ysql_impl/sql_txn/order_status_txn.h"
#include "ysql_impl/sql_txn/payment_txn.h"
#include "ysql_impl/sql_txn/popular_item_txn.h"
#include "ysql_impl/sql_txn/related_customer_txn.h"
#include "ysql_impl/sql_txn/stock_level_txn.h"
#include "ysql_impl/sql_txn/top_balance_txn.h"

namespace ydb_util {
class YSQLParser : public Parser {
 public:
  YSQLParser(const std::string& file_name, pqxx::connection* conn)
      : Parser(file_name), conn_(conn) {}
  virtual ~YSQLParser() = default;

 protected:
  Txn* GetTxnPtr_(char c) noexcept override {
    Txn* txn = nullptr;
    switch (c) {
      case 'N': {
        // New Order
        txn = new YSQLNewOrderTxn(conn_);
        break;
      }
      case 'P': {
        // Payment
        // Unimplemented yet
        LOG_INFO << "Payment";
        txn = new YSQLPaymentTxn(conn_);
        break;
      }
      case 'D': {
        // Delivery
        txn = new YSQLDeliveryTxn(conn_);
        break;
      }
      case 'O': {
        // Order-Status
        txn = new YSQLOrderStatusTxn(conn_);
        break;
      }
      case 'S': {
        // Stock-Level
        txn = new YSQLStockLevelTxn(conn_);
        break;
      }
      case 'I': {
        // Popular-Item
        txn = new YSQLPopularItemTxn(conn_);
        break;
      }
      case 'T': {
        // Top-Balance
        txn = new YSQLTopBalanceTxn(conn_);
        break;
      }
      case 'R': {
        // Related-Customer
        txn = new YSQLRelatedCustomerTxn(conn_);
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
  pqxx::connection* conn_;
};
}  // namespace ydb_util

#endif