#ifndef YDB_PERF_SQL_DRIVER_H_
#define YDB_PERF_SQL_DRIVER_H_
#include <pqxx/pqxx>
#include <string>

#include "common/parser/parser.h"
#include "common/txn/txn_type.h"
#include "common/util/status.h"

namespace ysql_impl {
class SQLDriver {
  using Status = ydb_util::Status;
  using Txn = ydb_util::Txn<pqxx::connection>;

 public:
  SQLDriver(const std::string host, const std::string port,
            const std::string user, const std::string password,
            const std::string dbname, const std::string sslmode,
            const std::string sslrootcert, int idx)
      : HOST(host),
        PORT(port),
        USER(user),
        PASSWORD(password),
        DB_NAME(dbname),
        SSL_MODE(sslmode),
        SSL_ROOT_CERT(sslrootcert) {
    idx_ = idx;
  }

  Status operator()() {
    std::string filename = xactDir + std::to_string(idx_) + ".txt";
    pqxx::connection* conn = NULL;
    conn = connect();
    if (conn == NULL) {
      return Status::ConnectionFailed();
    }
    ydb_util::Parser<pqxx::connection> parser(filename, conn);
    auto s = parser.Init();
    if (!s.ok()) {
      return s;
    }
    while (true) {
      Txn* t = nullptr;
      if (parser.GetNextTxn(&t).isEndOfFile()) {
        break;
      }
      std::cout << "start to sql" << std::endl;
      t->ExecuteSQL();
    }
    return Status::OK();
  }

  pqxx::connection* connect() {
    std::string url = "host=" + HOST + " port=" + PORT + " dbname=" + DB_NAME +
                      " user=" + USER + " password=" + PASSWORD;

    if (SSL_MODE != "") {
      url += " sslmode=" + SSL_MODE;

      if (SSL_ROOT_CERT != "") {
        url += " sslrootcert=" + SSL_ROOT_CERT;
      }
    }

    std::cout << ">>>> Connecting to YugabyteDB!" << std::endl;

    pqxx::connection* conn = new pqxx::connection(url);

    std::cout << ">>>> Successfully connected to YugabyteDB!" << std::endl;

    return conn;
  }

 private:
  std::string HOST, PORT, DB_NAME, USER, PASSWORD, SSL_MODE, SSL_ROOT_CERT;
  static std::string xactDir;
  int idx_;
};
std::string SQLDriver::xactDir = "data/xact-files/";

};  // namespace ysql_impl
#endif