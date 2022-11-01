#include "ThreadPool.h"
#include "common/parser/parser.h"
#include "common/util/env_util.h"
#include "ycql_impl/cql_driver.h"

const std::string HOST = ydb_util::getenv("YDB_HOST", "127.0.0.1");
const std::string SERVER_NUM = ydb_util::getenv("YDB_SERVER_NUM", "5");
const std::string SERVER_IDX = ydb_util::getenv("YDB_SERVER_IDX", "0");
const std::string TXN_NUM = ydb_util::getenv("YDB_TXN_NUM", "20");

const std::string DB_NAME = "yugabyte";
const std::string USER = "yugabyte";
const std::string PASSWORD = "yugabyte";
const std::string SSL_MODE = "";
const std::string SSL_ROOT_CERT = "";

// Create a new cluster.
CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  auto rc = cass_cluster_set_contact_points(cluster, hosts);
  assert(rc == CASS_OK);
  return cluster;
}

int main(int argc, char* argv[]) {
  ThreadPool pool(5);
  int idx = std::stoi(SERVER_IDX);
  int totalTxn = std::stoi(TXN_NUM);
  int serverNum = std::stoi(SERVER_NUM);
  CassCluster* cluster = nullptr;
  cluster = create_cluster(HOST.c_str());

  std::vector<std::future<ydb_util::Status>> results;

  for (; idx <= totalTxn; idx += serverNum) {
    results.push_back(pool.enqueue(ycql_impl::CQLDriver(HOST, cluster, idx)));
  }

  pool.JoinAll();

  int txnIdx = std::stoi(SERVER_IDX);
  for (auto& res : results) {
    std::cout << ydb_util::format("transaction %d's Status:%s", txnIdx,
                                  res.get().ToString().c_str())
              << std::endl;
    txnIdx += 5;
  }

  cass_cluster_free(cluster);

  return 0;
}