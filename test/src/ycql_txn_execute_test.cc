#include "gtest/gtest.h"
#include "ycql_impl/cql_driver.h"
#include "ycql_impl/cql_txn/delivery_txn.h"
#include "ycql_impl/cql_txn/new_order_txn.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  cass_future_free(future);

  return rc;
}

class CQLTxnExecuteTest : public ::testing::Test {
 public:
  void SetUp() override {
    std::cout << TEST_TXN_OUT_PATH << ", " << TEST_ERR_OUT_PATH << std::endl;
    txn_out_ = std::ofstream(TEST_TXN_OUT_PATH + "txn_out.out", std::ios::out);
    err_out_ = std::ofstream(TEST_ERR_OUT_PATH + "err_out.out", std::ios::out);
    EXPECT_TRUE(txn_out_.is_open());
    EXPECT_TRUE(err_out_.is_open());
    LOG_INFO << "connecting to server...";
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, hosts);
    conn = cass_session_new();
    EXPECT_EQ(connect_session(conn, cluster), CASS_OK);
  }

 protected:
  CassSession* conn = nullptr;
  std::ofstream txn_out_;
  std::ofstream err_out_;
  static constexpr char hosts[] = "127.0.0.1";
};

TEST_F(CQLTxnExecuteTest, NewOrderTest1) {
  ydb_util::Txn* txn = new YCQLNewOrderTxn(conn, txn_out_, err_out_);
  auto new_order_txn = dynamic_cast<YCQLNewOrderTxn*>(txn);
  // TODO(ZjuYTW): populate new_order_txn and test
  new_order_txn->c_id_ = 1;
  new_order_txn->d_id_ = 1;
  new_order_txn->w_id_ = 1;
  std::vector<std::string> orders;
  for (int i = 0; i < 4; i++) {
    std::string str;
    // item_id
    str += std::to_string(i + 1) + ",";
    // supply_warehouse id
    str += std::to_string(1) + ",";
    // quantity
    str += std::to_string(5);
    new_order_txn->orders_.push_back(std::move(str));
  }

  double elapsedTime;
  auto st = new_order_txn->Execute(&elapsedTime);
  LOG_INFO << st.ToString();
  ASSERT_TRUE(st.ok());
}

TEST_F(CQLTxnExecuteTest, DeliveryTxnTest) {
  auto delivery_txn = YCQLDeliveryTxn(conn, txn_out_, err_out_);
  delivery_txn.carrier_id_ = 1;
  delivery_txn.w_id_ = 1;
  double elapsedTime;
  auto st = delivery_txn.Execute(&elapsedTime);
  ASSERT_TRUE(st.ok());
}

TEST_F(CQLTxnExecuteTest, OrderStatusTxnTest) {
  auto order_status_txn = YCQLOrderStatusTxn(conn, txn_out_, err_out_);
  order_status_txn.c_d_id_ = 1;
  order_status_txn.c_w_id_ = 1;
  order_status_txn.c_id_ = 1;
  double elapsedTime;
  auto st = order_status_txn.Execute(&elapsedTime);
  ASSERT_TRUE(st.ok());
}

TEST_F(CQLTxnExecuteTest, PaymentTxnTest) {
  auto payment_txn = YCQLPaymentTxn(conn, txn_out_, err_out_);
  payment_txn.c_id_ = 1;
  payment_txn.d_id_ = 1;
  payment_txn.w_id_ = 1;
  double elapsedTime;
  auto st = payment_txn.Execute(&elapsedTime);
  ASSERT_TRUE(st.ok());
}

TEST_F(CQLTxnExecuteTest, PopularItemTxnTest) {
  auto popular_item_txn = YCQLPopularItemTxn(conn, txn_out_, err_out_);
  popular_item_txn.w_id_ = 1;
  popular_item_txn.d_id_ = 1;
  popular_item_txn.l_ = 2;
  double elapsedTime;
  auto st = popular_item_txn.Execute(&elapsedTime);
  if (!st.ok()) {
    LOG_DEBUG << st.ToString();
  }
  ASSERT_TRUE(st.ok());
}

TEST_F(CQLTxnExecuteTest, RelatedCustomerTxnTest) {
  ydb_util::Txn* txn = new YCQLRelatedCustomerTxn(conn, txn_out_, err_out_);
  auto related_customer_txn = dynamic_cast<YCQLRelatedCustomerTxn*>(txn);
  ASSERT_NE(related_customer_txn, nullptr);

  related_customer_txn->c_w_id_ = 1;
  related_customer_txn->c_d_id_ = 1;
  related_customer_txn->c_id_ = 4;

  double elapsedTime;
  auto st = related_customer_txn->Execute(&elapsedTime);
  LOG_INFO << st.ToString();
  ASSERT_TRUE(st.ok());
}

TEST_F(CQLTxnExecuteTest, StockLevelTxnTest) {
  ydb_util::Txn* txn = new YCQLStockLevelTxn(conn, txn_out_, err_out_);
  auto stock_level_txn = dynamic_cast<YCQLStockLevelTxn*>(txn);
  ASSERT_NE(stock_level_txn, nullptr);

  stock_level_txn->w_id_ = 1;
  stock_level_txn->d_id_ = 1;
  stock_level_txn->l_ = 2;
  stock_level_txn->t_ = 60;

  double elapsedTime;
  auto st = stock_level_txn->Execute(&elapsedTime);
  LOG_INFO << st.ToString();
  ASSERT_TRUE(st.ok());
}

TEST_F(CQLTxnExecuteTest, TopBalanceTxnTest) {
  ydb_util::Txn* txn = new YCQLTopBalanceTxn(conn, txn_out_, err_out_);
  auto top_balance_txn = dynamic_cast<YCQLTopBalanceTxn*>(txn);
  ASSERT_NE(top_balance_txn, nullptr);
  double elapsedTime;
  auto st = top_balance_txn->Execute(&elapsedTime);
  LOG_INFO << st.ToString();
  ASSERT_TRUE(st.ok());
}
}  // namespace ycql_impl