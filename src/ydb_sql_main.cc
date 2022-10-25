#include <pqxx/pqxx>

#include "ThreadPool.h"
#include "common/util/env_util.h"
#include "ysql_impl/sql_driver.h"

const std::string HOST = ydb_util::getenv("YDB_HOST", "127.0.0.1");
const std::string PORT = ydb_util::getenv("YDB_SQL_PORT", "5433");
const std::string SERVER_NUM = ydb_util::getenv("YDB_SERVER_NUM", "5");
const std::string SERVER_IDX = ydb_util::getenv("YDB_SERVER_IDX", "0");
const std::string TXN_NUM = ydb_util::getenv("YDB_TXN_NUM", "20");

const std::string DB_NAME = "yugabyte";
const std::string USER = "yugabyte";
const std::string PASSWORD = "yugabyte";
const std::string SSL_MODE = "";
const std::string SSL_ROOT_CERT = "";

int main(int argc, char* argv[]) {
  ThreadPool pool(10);
  int idx = std::stoi(SERVER_IDX);
  int totalTxn = std::stoi(TXN_NUM);
  for (; idx <= totalTxn; idx += 5) {
    auto ret_feature = pool.enqueue(ysql_impl::SQLDriver(
        HOST, PORT, USER, PASSWORD, DB_NAME, SSL_MODE, SSL_ROOT_CERT, idx));
    std::cout << ret_feature.get().ToString() << std::endl;
  }
  pool.JoinAll();
  return 0;
}