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

TEST_F(CQLNewOrderTxnTest, Test1) { 
  YCQLNewOrderTxn new_order_txn(conn); 
  // TODO(ZjuYTW): populate new_order_txn and test
}
}  // namespace ycql_impl