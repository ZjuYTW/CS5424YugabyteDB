#include "common/parser/parser.h"
#include "gtest/gtest.h"

namespace ydb_util {

TEST(TxnArgsParserTest, new_order) {
  Parser<CassSession> parser("data/xact_files/new_order_txn.txt",
                                       nullptr);
  auto s = parser.Init();
  std::cerr << s.ToString() << std::endl;
  EXPECT_EQ(s.ok(), true);
  Txn<CassSession> *t = nullptr;

  while (!parser.GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *newOrderTxn = dynamic_cast<NewOrderTxn<CassSession> *>(t);
    EXPECT_NE(newOrderTxn, nullptr);
    EXPECT_EQ(newOrderTxn->c_id_, 1);
    EXPECT_EQ(newOrderTxn->w_id_, 1);
    EXPECT_EQ(newOrderTxn->d_id_, 1);
    EXPECT_EQ(newOrderTxn->orders_.size(), 2);
  }
}

// add FRIEND_TEST in each Txn template class first if you want to access private members
// add sample input files under {WORKSPACE}/data/xact_files/{TXN_NAME}_txn.txt
/* pending UT tests
TEST(TxnArgsParserTest, payment) {}
TEST(TxnArgsParserTest, delivery) {}
TEST(TxnArgsParserTest, order_status) {}
TEST(TxnArgsParserTest, stock_level) {}
*/

TEST(TxnArgsParserTest, popular_item) {
  Parser<CassSession> parser("data/xact_files/popular_item_txn.txt",
                             nullptr);
  auto s = parser.Init();
  std::cerr << s.ToString() << std::endl;
  EXPECT_EQ(s.ok(), true);
  Txn<CassSession> *t = nullptr;

  while (!parser.GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *popularItemTxn = dynamic_cast<PopularItemTxn<CassSession> *>(t);
    EXPECT_NE(popularItemTxn, nullptr);
    EXPECT_EQ(popularItemTxn->w_id_, 1);
    EXPECT_EQ(popularItemTxn->d_id_, 1);
    EXPECT_EQ(popularItemTxn->l_, 1);
  }
}

TEST(TxnArgsParserTest, top_balance) {
  Parser<CassSession> parser("data/xact_files/top_balance_txn.txt",
                             nullptr);
  auto s = parser.Init();
  std::cerr << s.ToString() << std::endl;
  EXPECT_EQ(s.ok(), true);
  Txn<CassSession> *t = nullptr;

  while (!parser.GetNextTxn(&t).isEndOfFile()) {
    EXPECT_NE(t, nullptr);
    auto *topBalanceTxn = dynamic_cast<TopBalanceTxn<CassSession> *>(t);
    EXPECT_NE(topBalanceTxn, nullptr);
  }
}

TEST(TxnArgsParserTest, related_customer) {
    Parser<CassSession> parser("data/xact_files/related_customer_txn.txt",
                               nullptr);
    auto s = parser.Init();
    std::cerr << s.ToString() << std::endl;
    EXPECT_EQ(s.ok(), true);
    Txn<CassSession> *t = nullptr;

    while (!parser.GetNextTxn(&t).isEndOfFile()) {
        EXPECT_NE(t, nullptr);
        auto *relatedCustomerTxn = dynamic_cast<RelatedCustomerTxn<CassSession> *>(t);
        EXPECT_NE(relatedCustomerTxn, nullptr);
        EXPECT_EQ(relatedCustomerTxn->c_w_id_, 1);
        EXPECT_EQ(relatedCustomerTxn->c_d_id_, 1);
        EXPECT_EQ(relatedCustomerTxn->c_id_, 1);
    }
}

}