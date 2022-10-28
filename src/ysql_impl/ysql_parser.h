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
  YSQLParser(const std::string& file_name, std::ofstream& txn_out,
             std::ofstream& out_err_fs, pqxx::connection* conn)
      : Parser(file_name),
        conn_(conn),
        txn_out_(txn_out),
        err_out_(out_err_fs) {}
  virtual ~YSQLParser() = default;

 protected:
  Txn* GetTxnPtr_(char c) noexcept override {
    Txn* txn = nullptr;
    switch (c) {
      case 'N': {
        // New Order
        txn = new YSQLNewOrderTxn(conn_, txn_out_, err_out_);
        break;
      }
      case 'P': {
        // Payment
        // Unimplemented yet
        LOG_INFO << "Payment";
        txn = new YSQLPaymentTxn(conn_,txn_out_,err_out_);
        break;
      }
      case 'D': {
        // Delivery
        LOG_INFO << "Delivery";
        txn = new YSQLDeliveryTxn(conn_,txn_out_,err_out_);
        break;
      }
      case 'O': {
        // Order-Status
        txn = new YSQLOrderStatusTxn(conn_,txn_out_,err_out_);
        break;
      }
      case 'S': {
        // Stock-Level
        txn = new YSQLStockLevelTxn(conn_,txn_out_,err_out_);
        break;
      }
      case 'I': {
        // Popular-Item
        txn = new YSQLPopularItemTxn(conn_,txn_out_,err_out_);
        break;
      }
      case 'T': {
        // Top-Balance
        txn = new YSQLTopBalanceTxn(conn_,txn_out_,err_out_);
        break;
      }
      case 'R': {
        // Related-Customer
        txn = new YSQLRelatedCustomerTxn(conn_,txn_out_,err_out_);
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
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ydb_util

#endif