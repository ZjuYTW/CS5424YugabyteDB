#include "common/parser/parser.h"

namespace ydb_util {
Status Parser::GetNextTxn(Txn** txn) noexcept {
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

Txn* Parser::GetTxnPtr_(char c) noexcept {
  Txn* txn = nullptr;
  switch (c) {
    case 'N': {
      // New Order
      txn = new NewOrderTxn();
      break;
    }
    case 'P': {
      // Payment
      txn = new PaymentTxn();
      break;
    }
    case 'D': {
      // Delivery
      txn = new DeliveryTxn();
      break;
    }
    case 'O': {
      // Order-Status
      txn = new OrderStatusTxn();
      break;
    }
    case 'S': {
      // Stock-Level
      txn = new StockLevelTxn();
      break;
    }
    case 'I': {
      // Popular-Item
      txn = new PopularItemTxn();
      break;
    }
    case 'T': {
      // Top-Balance
      txn = new TopBalanceTxn();
      break;
    }
    case 'R': {
      // Related_Customer
      txn = new RelatedCustomerTxn();
      break;
    }
    default: {
      // Unreachable
      assert(false);
    }
  }
  return txn;
}

}  // namespace ydb_util