#include "gtest/gtest.h"
#include "ycql_impl/cql_driver.h"
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
    txn_out_ = std::ofstream(TEST_TXN_OUT_PATH + "txn_out.out", std::ios::out);
    err_out_ = std::ofstream(TEST_ERR_OUT_PATH + "err_out.out", std::ios::out);
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

<<<<<<< HEAD:test/src/ycql_new_order_txn_test.cc
TEST_F(CQLNewOrderTxnTest, NewOrderTest1) {
  ydb_util::Txn* txn = new YCQLNewOrderTxn(conn, txn_out_, err_out_);
=======
TEST_F(CQLTxnExecuteTest, NewOrderTest1) {
  ydb_util::Txn* txn = new YCQLNewOrderTxn(conn);
>>>>>>> e0cd184 (framework for txn execute test):test/src/ycql_txn_execute_test.cc
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

TEST_F(CQLTxnExecuteTest, DeliveryTxnTest) {}

TEST_F(CQLTxnExecuteTest, OrderStatusTxnTest) {}

TEST_F(CQLTxnExecuteTest, PaymentTxnTest) {}

TEST_F(CQLTxnExecuteTest, PopularItemTxnTest) {}

TEST_F(CQLTxnExecuteTest, RelatedCustomerTxnTest) {}

TEST_F(CQLTxnExecuteTest, StockLevelTxnTest) {}

TEST_F(CQLTxnExecuteTest, TopBalanceTxnTest) {}
}  // namespace ycql_impl