#include "common/parser/parser.h"
#include "gtest/gtest.h"
#include "gtest/gtest_prod.h"
#include "common/txn/new_order_txn.h"

namespace ydb_util {

TEST(TxnArgsParserTest, new_order) {
  Parser<CassSession> parser("data/xact_files/new_order_txn.txt",
                                       nullptr);
  auto s = parser.Init();
  std::cerr << s.ToString() << std::endl;
  EXPECT_EQ(s.ok(), true);
  Txn<CassSession> *t = nullptr;
  auto n = NewOrderTxn<CassSession>(nullptr);

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

}