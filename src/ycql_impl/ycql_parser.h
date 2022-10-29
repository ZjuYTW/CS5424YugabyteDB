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

namespace ycql_impl {
class YCQLParser : public ydb_util::Parser {
  using Txn = ydb_util::Txn;

 public:
  YCQLParser(const std::string& file_name, CassSession* session,
             std::ofstream& txn_out, std::ofstream& out_err_fs)
      : Parser(file_name),
        conn_(session),
        txn_out_(txn_out),
        err_out_(out_err_fs) {}

  virtual ~YCQLParser() = default;

 protected:
  Txn* GetTxnPtr_(char c) noexcept override {
    Txn* txn = nullptr;
    switch (c) {
      case 'N': {
        // New Order
        txn = new YCQLNewOrderTxn(conn_, txn_out_, err_out_);
        break;
      }
      case 'P': {
        // Payment
        // Unimplemented yet
        LOG_INFO << "Payment";
        txn = new YCQLPaymentTxn(conn_, txn_out_, err_out_);
        break;
      }
      case 'D': {
        // Delivery
        txn = new YCQLDeliveryTxn(conn_, txn_out_, err_out_);
        break;
      }
      case 'O': {
        // Order-Status
        txn = new YCQLOrderStatusTxn(conn_, txn_out_, err_out_);
        break;
      }
      case 'S': {
        // Stock-Level
        txn = new YCQLStockLevelTxn(conn_, txn_out_, err_out_);
        break;
      }
      case 'I': {
        // Popular-Item
        txn = new YCQLPopularItemTxn(conn_, txn_out_, err_out_);
        break;
      }
      case 'T': {
        // Top-Balance
        txn = new YCQLTopBalanceTxn(conn_, txn_out_, err_out_);
        break;
      }
      case 'R': {
        // Related-Customer
        txn = new YCQLRelatedCustomerTxn(conn_, txn_out_, err_out_);
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
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
}  // namespace ycql_impl
#endif