#ifndef YDB_PERF_TYPE_H_
#define YDB_PERF_TYPE_H_

#include <fstream>

#include "common/util/status.h"

namespace ydb_util {
enum class TxnType {
  new_order,
  payment,
  delivery,
  order_status,
  stock_level,
  popular_item,
  top_balance,
  related_customer,
};

class Txn {
 public:
  explicit Txn(TxnType type) : txn_type_(type) {}

  virtual Status Execute() noexcept = 0;

  virtual Status Init(const std::string& first_line,
                      std::ifstream& ifs) noexcept = 0;

 private:
  TxnType txn_type_;
};

class NewOrderTxn : public Txn {
 public:
  explicit NewOrderTxn() : Txn(TxnType::new_order) {}

  Status Execute() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

class PaymentTxn : public Txn {
 public:
  explicit PaymentTxn() : Txn(TxnType::payment) {}

  Status Execute() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

class DeliveryTxn : public Txn {
 public:
  explicit DeliveryTxn() : Txn(TxnType::delivery) {}

  Status Execute() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

class OrderStatusTxn : public Txn {
 public:
  explicit OrderStatusTxn() : Txn(TxnType::order_status) {}

  Status Execute() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

class StockLevelTxn : public Txn {
 public:
  explicit StockLevelTxn() : Txn(TxnType::stock_level) {}

  Status Execute() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

class PopularItemTxn : public Txn {
 public:
  explicit PopularItemTxn() : Txn(TxnType::popular_item) {}

  Status Execute() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

class TopBalanceTxn : public Txn {
 public:
  explicit TopBalanceTxn() : Txn(TxnType::top_balance) {}

  Status Execute() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};

class RelatedCustomerTxn : public Txn {
 public:
  explicit RelatedCustomerTxn() : Txn(TxnType::related_customer) {}

  Status Execute() noexcept override;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override;
};
};  // namespace ydb_util

#endif