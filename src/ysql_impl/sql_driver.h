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
    std::string outputMeasure= outDir+"/measure_log/sql_"+std::to_string(idx_)+".out";
    std::string outputTXN= outDir+"/txn_log/sql_"+std::to_string(idx_)+".out";
    std::string outputErr= outDir+"/err_log/sql_"+std::to_string(idx_)+".out";

    auto out_txn_fs = std::ofstream(outputTXN, std::ios::out);
    auto out_measure_fs = std::ofstream(outputMeasure, std::ios::out);
    auto out_err_fs = std::ofstream(outputErr,std::ios::out);

    pqxx::connection* conn = nullptr;
    conn = connect();
    if (conn == nullptr) {
      return Status::ConnectionFailed();
    }
    std::unique_ptr<ydb_util::Parser> parser_p =
        std::make_unique<ydb_util::YSQLParser>(filename,out_txn_fs,out_err_fs, conn);
    auto s = parser_p->Init();
    if (!s.ok()) {
      return s;
    }
    while (true) {
      Txn* t = nullptr;
      if (parser_p->GetNextTxn(&t).isEndOfFile()) {
        break;
      }
      double processTime = -1;
      t->Execute(&processTime);
      if (processTime != -1) {
        elapsedTime.push_back(processTime);
      }
    }
    sort(elapsedTime.begin(), elapsedTime.end());

    float totalTime = 0;
    for (float i : elapsedTime) {
      totalTime += i;
    }

    out_measure_fs
        << "Total number of transactions processed: " << elapsedTime.size() << "\n"
        << "Total elapsed time for processing the transactions: " << totalTime << "\n"
        << "Transaction throughput: " << elapsedTime.size()/totalTime << "\n"
        << "Average transaction latency: " << totalTime * 60 / elapsedTime.size() << "\n"
        << "Median transaction latency: " << elapsedTime[elapsedTime.size()*0.5] * 60 << "\n"
        << "95th percentile transaction latency: " << elapsedTime[elapsedTime.size()*0.95] * 60 << "\n"
        << "99th percentile transaction latency: " << elapsedTime[elapsedTime.size()*0.99] * 60<< std::endl;

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
  std::vector<float> elapsedTime;
};
std::string SQLDriver::xactDir = "data/test_xact_files/";
std::string SQLDriver::outDir = "data/output/";

}
#endif