#include <pqxx/pqxx>

#include "ThreadPool.h"
#include "ysql_impl/sql_driver.h"

const std::string HOST = "127.0.0.1";
const std::string PORT = "5433";
const std::string DB_NAME = "yugabyte";
const std::string USER = "yugabyte";
const std::string PASSWORD = "yugabyte";
const std::string SSL_MODE = "";
const std::string SSL_ROOT_CERT = "";

int main(int argc, char* argv[]) {
  ThreadPool pool(10);
  int idx = 1;
  auto ret_feature = pool.enqueue(ysql_impl::SQLDriver(
      HOST, PORT, USER, PASSWORD, DB_NAME, SSL_MODE, SSL_ROOT_CERT, idx));
  pool.JoinAll();
  std::cout << ret_feature.get().ToString() << std::endl;
  return 0;
}