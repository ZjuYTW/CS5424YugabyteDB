#include "ThreadPool.h"
#include "common/parser/parser.h"
#include "ycql_impl/cql_driver.h"

// Create a new cluster.
CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

int main(int argc, char* argv[]) {
  // TODO(ZjuYTW): Figure out what args are we need
  // Now just a simple test code
  if (argc != 2) {
    std::cout << "We expect 1 arg for input file name..." << std::endl;
    return 1;
  }
  std::string file_name = argv[1];
  // Thread pool of size 10 => we could change it to match
  // our txt file number later
  ThreadPool pool(10);
  std::string ip = "127.0.0.1";
  int idx = 1;
  CassCluster* cluster = nullptr;
  cluster = create_cluster(ip.c_str());
  std::cout << "Enque..." << std::endl;
  pool.enqueue(ycql_impl::CQLDriver(ip, cluster, idx));
}