#include <pqxx/pqxx>

#include "ThreadPool.h"
#include "common/util/env_util.h"
#include "ysql_impl/sql_driver.h"

const std::string HOST = ydb_util::getenv("YDB_HOST", "127.0.0.1");
const std::string PORT = ydb_util::getenv("YDB_SQL_PORT", "5433");
const std::string DB_NAME = "yugabyte";
const std::string USER = "yugabyte";
const std::string PASSWORD = "yugabyte";
const std::string SSL_MODE = "";
const std::string SSL_ROOT_CERT = "";

int main(int argc, char* argv[]) {
  // DEMO: Since the file name is not set in main now, suppose main accept sql
  // or cql now.
  //  if (argc != 2) {
  //    std::cout << "We expect 1 arg for sql or cql" << std::endl;
  //    return 1;
  //  }
  // std::string db_type = argv[1];
  // Thread pool of size 10 => we could change it to match
  // our txt file number later
  ThreadPool pool(10);
  int idx = 1;
  //  if (db_type == "cql") {
  //    CassCluster* cluster = nullptr;
  //    cluster = create_cluster(HOST.c_str());
  //    auto ret_feature = pool.enqueue(ycql_impl::CQLDriver(HOST, cluster,
  //    idx)); pool.JoinAll(); cass_cluster_free(cluster);
  //    assert(ret_feature.get().isConnectionFailed());
  //  } else if (db_type == "sql") {
  auto ret_feature = pool.enqueue(ysql_impl::SQLDriver(
      HOST, PORT, USER, PASSWORD, DB_NAME, SSL_MODE, SSL_ROOT_CERT, idx));
  pool.JoinAll();
  std::cout << ret_feature.get().ToString() << std::endl;
  //  } else {
  //    std::cout << "We expect 1 arg for sql or cql" << std::endl;
  //    return 1;
  //  }

  return 0;
}