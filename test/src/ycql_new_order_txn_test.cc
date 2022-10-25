#include "gtest/gtest.h"
#include "ycql_impl/cql_driver.h"

namespace ycql_impl {
CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  cass_future_free(future);

  return rc;
}

class CQLNewOrderTxnTest : public ::testing::Test {
 public:
  void SetUp() override {
    LOG_INFO << "connecting to server...";
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, hosts);
    conn = cass_session_new();
    EXPECT_EQ(connect_session(conn, cluster), CASS_OK);
  }

 protected:
  CassSession* conn = nullptr;
  static constexpr char hosts[] = "127.0.0.1";
};

TEST_F(CQLNewOrderTxnTest, NewOrderTest1) { 
  ydb_util::Txn *txn = new YCQLNewOrderTxn(conn);
  auto new_order_txn = dynamic_cast<YCQLNewOrderTxn*>(txn);
  // TODO(ZjuYTW): populate new_order_txn and test
  new_order_txn->c_id_ = 1;
  new_order_txn->d_id_ = 1;
  new_order_txn->w_id_ = 1;
  std::vector<std::string> orders;
  for(int i = 0; i < 4; i++){
    std::string str;
    // item_id
    str += std::to_string(i+1) + ",";
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
}  // namespace ycql_impl