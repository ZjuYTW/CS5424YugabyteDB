#include "common/util/logger.h"
#include "gtest/gtest.h"
#include "ycql_impl/ycql_parser.h"

namespace ycql_impl {

const std::string TEST_FILE_PATH = "data/xact_files/";

using namespace ydb_util;

TEST(TxnArgsParserTest, new_order) {
  std::unique_ptr<Parser> parser = std::make_unique<YCQLParser>(
      TEST_FILE_PATH + "new_order_txn.txt", nullptr);
  auto s = parser->Init();
  LOG_INFO << s.ToString();
  EXPECT_EQ(s.ok(), true);
  std::unique_ptr<Txn> t = nullptr;

  while (!parser->GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *newOrderTxn = dynamic_cast<YCQLNewOrderTxn *>(t.get());
    EXPECT_NE(newOrderTxn, nullptr);
    EXPECT_EQ(newOrderTxn->c_id_, 1);
    EXPECT_EQ(newOrderTxn->w_id_, 2);
    EXPECT_EQ(newOrderTxn->d_id_, 3);
    EXPECT_EQ(newOrderTxn->orders_.size(), 2);
  }
}

TEST(TxnArgsParserTest, payment) {
  std::unique_ptr<Parser> parser =
      std::make_unique<YCQLParser>(TEST_FILE_PATH + "payment_txn.txt", nullptr);
  auto s = parser->Init();
  LOG_INFO << s.ToString();
  EXPECT_EQ(s.ok(), true);
  std::unique_ptr<Txn> t = nullptr;

  if (!parser->GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *paymentTxn = dynamic_cast<YCQLPaymentTxn *>(t.get());
    EXPECT_NE(paymentTxn, nullptr);
    EXPECT_EQ(paymentTxn->w_id_, 1);
    EXPECT_EQ(paymentTxn->d_id_, 5);
    EXPECT_EQ(paymentTxn->c_id_, 2817);
    EXPECT_EQ(paymentTxn->payment_, 3849.58);
  }
}

TEST(TxnArgsParserTest, delivery) {
  std::unique_ptr<Parser> parser = std::make_unique<YCQLParser>(
      TEST_FILE_PATH + "delivery_txn.txt", nullptr);
  auto s = parser->Init();
  LOG_INFO << s.ToString();
  EXPECT_EQ(s.ok(), true);
  std::unique_ptr<Txn> t = nullptr;

  while (!parser->GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *deliveryTxn = dynamic_cast<YCQLDeliveryTxn *>(t.get());
    EXPECT_NE(deliveryTxn, nullptr);
    EXPECT_EQ(deliveryTxn->w_id_, 1);
    EXPECT_EQ(deliveryTxn->carrier_id_, 2);
  }
}

TEST(TxnArgsParserTest, order_status) {
  std::unique_ptr<Parser> parser = std::make_unique<YCQLParser>(
      TEST_FILE_PATH + "order_status.txt", nullptr);
  auto s = parser->Init();
  LOG_INFO << s.ToString();
  EXPECT_EQ(s.ok(), true);
  std::unique_ptr<Txn> t = nullptr;

  while (!parser->GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *orderStatusTxn = dynamic_cast<YCQLOrderStatusTxn *>(t.get());
    EXPECT_NE(orderStatusTxn, nullptr);
    EXPECT_EQ(orderStatusTxn->c_w_id_, 1);
    EXPECT_EQ(orderStatusTxn->c_d_id_, 2);
    EXPECT_EQ(orderStatusTxn->c_id_, 3);
  }
}

TEST(TxnArgsParserTest, stock_level) {
  std::unique_ptr<Parser> parser =
      std::make_unique<YCQLParser>(TEST_FILE_PATH + "stock_level.txt", nullptr);
  auto s = parser->Init();
  LOG_INFO << s.ToString();
  EXPECT_EQ(s.ok(), true);
  std::unique_ptr<Txn> t = nullptr;

  while (!parser->GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *stockLevelTxn = dynamic_cast<YCQLStockLevelTxn *>(t.get());
    EXPECT_NE(stockLevelTxn, nullptr);
    EXPECT_EQ(stockLevelTxn->w_id_, 1);
    EXPECT_EQ(stockLevelTxn->d_id_, 2);
    EXPECT_EQ(stockLevelTxn->t_, 3);
    EXPECT_EQ(stockLevelTxn->l_, 4);
  }
}

TEST(TxnArgsParserTest, popular_item) {
  std::unique_ptr<Parser> parser = std::make_unique<YCQLParser>(
      TEST_FILE_PATH + "popular_item_txn.txt", nullptr);
  auto s = parser->Init();
  LOG_INFO << s.ToString();
  EXPECT_EQ(s.ok(), true);
  std::unique_ptr<Txn> t = nullptr;

  while (!parser->GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *popularItemTxn = dynamic_cast<YCQLPopularItemTxn *>(t.get());
    EXPECT_NE(popularItemTxn, nullptr);
    EXPECT_EQ(popularItemTxn->w_id_, 1);
    EXPECT_EQ(popularItemTxn->d_id_, 2);
    EXPECT_EQ(popularItemTxn->l_, 3);
  }
}

TEST(TxnArgsParserTest, top_balance) {
  std::unique_ptr<Parser> parser = std::make_unique<YCQLParser>(
      TEST_FILE_PATH + "top_balance_txn.txt", nullptr);
  auto s = parser->Init();
  LOG_INFO << s.ToString();
  EXPECT_EQ(s.ok(), true);
  std::unique_ptr<Txn> t = nullptr;

  while (!parser->GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *topBalanceTxn = dynamic_cast<YCQLTopBalanceTxn *>(t.get());
    EXPECT_NE(topBalanceTxn, nullptr);
  }
}

TEST(TxnArgsParserTest, related_customer) {
  std::unique_ptr<Parser> parser = std::make_unique<YCQLParser>(
      "data/xact_files/related_customer_txn.txt", nullptr);
  auto s = parser->Init();
  LOG_INFO << s.ToString();
  EXPECT_EQ(s.ok(), true);
  std::unique_ptr<Txn> t = nullptr;

  while (!parser->GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *relatedCustomerTxn = dynamic_cast<YCQLRelatedCustomerTxn *>(t.get());
    EXPECT_NE(relatedCustomerTxn, nullptr);
    EXPECT_EQ(relatedCustomerTxn->c_w_id_, 1);
    EXPECT_EQ(relatedCustomerTxn->c_d_id_, 2);
    EXPECT_EQ(relatedCustomerTxn->c_id_, 3);
  }
}

}  // namespace ycql_impl