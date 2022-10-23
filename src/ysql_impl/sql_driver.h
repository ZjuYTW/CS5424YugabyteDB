#ifndef YDB_PERF_SQL_DRIVER_H_
#define YDB_PERF_SQL_DRIVER_H_
#include <memory>
#include <string>

#include "common/txn/txn_type.h"
#include "common/util/status.h"
#include "ysql_impl/ysql_parser.h"

namespace ysql_impl {
class SQLDriver {
  using Status = ydb_util::Status;
  using Txn = ydb_util::Txn;

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
    std::string outputMeasure= outDir+"measure_"+std::to_string(idx_)+".out";
    std::string outputTXn= outDir+"transaction_"+std::to_string(idx_)+".out";
    pqxx::connection* conn = nullptr;
    conn = connect();
    if (conn == nullptr) {
      return Status::ConnectionFailed();
    }
    std::unique_ptr<ydb_util::Parser> parser_p =
        std::make_unique<ydb_util::YSQLParser>(filename, conn);
    auto s = parser_p->Init();
    if (!s.ok()) {
      return s;
    }
    while (true) {
      Txn* t = nullptr;
      if (parser_p->GetNextTxn(&t).isEndOfFile()) {
        break;
      }
      t->Execute();
    }
    return Status::OK();
  }

  pqxx::connection* connect() {
    std::string url = "host=" + HOST + " port=" + PORT + " dbname=" + DB_NAME +
                      " user=" + USER + " password=" + PASSWORD;

    if (!SSL_MODE.empty()) {
      url += " sslmode=" + SSL_MODE;

      if (!SSL_ROOT_CERT.empty()) {
        url += " sslrootcert=" + SSL_ROOT_CERT;
      }
    }

    std::cout << ">>>> Connecting to YugabyteDB!" << std::endl;

    auto* conn = new pqxx::connection(url);

    std::cout << ">>>> Successfully connected to YugabyteDB!" << std::endl;

    return conn;
  }

 private:
  std::string HOST, PORT, DB_NAME, USER, PASSWORD, SSL_MODE, SSL_ROOT_CERT;
  static std::string xactDir,outDir;
  int idx_;
};
std::string SQLDriver::xactDir = "data/test_xact_files/";
std::string SQLDriver::outDir = "data/output/";

}
#endif