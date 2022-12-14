#ifndef YDB_PERF_SQL_DRIVER_H_
#define YDB_PERF_SQL_DRIVER_H_
#include <memory>
#include <string>

#include "common/txn/txn_type.h"
#include "common/util/status.h"
#include "ysql_impl/ysql_parser.h"

namespace ysql_impl {
#ifndef PROCESS_TXN_TIME
#define EXPAND                  \
  DO_ONE_TXN_(delivery)         \
  DO_ONE_TXN_(new_order)        \
  DO_ONE_TXN_(order_status)     \
  DO_ONE_TXN_(payment)          \
  DO_ONE_TXN_(popular_item)     \
  DO_ONE_TXN_(related_customer) \
  DO_ONE_TXN_(stock_level)      \
  DO_ONE_TXN_(top_balance)

#define INSERT_TXN_TIME                 \
  do {                                  \
    switch (t->GetTxnType()) { EXPAND } \
  } while (0);

#define PROCESS_TXN_TIME \
  do {                   \
    EXPAND               \
  } while (0);

#endif
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
    std::string outputMeasure =
        outDir + "/measure_log/sql_" + std::to_string(idx_) + ".out";
    std::string outputTXN =
        outDir + "/txn_log/sql_" + std::to_string(idx_) + ".out";
    std::string outputErr =
        outDir + "/err_log/sql_" + std::to_string(idx_) + ".out";
    srand(idx_);
    auto out_txn_fs = std::ofstream(outputTXN, std::ios::out);
    auto out_measure_fs = std::ofstream(outputMeasure, std::ios::out);
    auto out_err_fs = std::ofstream(outputErr, std::ios::out);

    pqxx::connection* conn = nullptr;
    conn = connect();
    if (conn == nullptr) {
      return Status::ConnectionFailed();
    }
    std::unique_ptr<ydb_util::Parser> parser_p =
        std::make_unique<ydb_util::YSQLParser>(filename, out_txn_fs, out_err_fs,
                                               conn);
    auto s = parser_p->Init();
    if (!s.ok()) {
      return s;
    }
    while (true) {
      // here we use smart ptr to avoid delete manully
      std::unique_ptr<Txn> t = nullptr;
      s = parser_p->GetNextTxn(&t);
      if (!s.ok()) {
        // EndOfFile or Something Bad
        break;
      }
      // retry in execute level
      double processTime;

      int retryCount = 0;
      while (retryCount < 5) {
        auto status = t->Execute(&processTime);
        if (status.ok()) {
          elapsedTime.push_back(processTime);
          break;
        } else {
          LOG_INFO << "retry on sql driver " << retryCount << " times";
          std::this_thread::sleep_for(
              std::chrono::milliseconds((1000) * retryCount));
          retryCount++;
        }
      }
      if (retryCount == 5) {
        // todo: if still not work, change to continue here
        LOG_INFO << "sql layer retry still failed";
        return Status::Invalid("sql layer retry still failed");
      }
#define DO_ONE_TXN_(txn_name)         \
  case ydb_util::TxnType::txn_name: { \
    txn_name.push_back(processTime);  \
    break;                            \
  }
      INSERT_TXN_TIME
#undef DO_ONE_TXN_
    }
    sort(elapsedTime.begin(), elapsedTime.end());

    double totalTime = 0;
    for (auto i : elapsedTime) {
      totalTime += i;
    }

    out_measure_fs << "Total number of transactions processed: "
                   << elapsedTime.size() << "\n"
                   << "Total elapsed time for processing the transactions: "
                   << totalTime / 1e9 << " seconds\n"
                   << "Transaction throughput: "
                   << elapsedTime.size() / totalTime * 1e9
                   << " transactions/second\n"
                   << "Average transaction latency: "
                   << totalTime / 1e9 / elapsedTime.size() << " seconds\n"
                   << "Median transaction latency: "
                   << elapsedTime[elapsedTime.size() * 0.5] / 1e9
                   << " seconds\n"
                   << "95th percentile transaction latency: "
                   << elapsedTime[elapsedTime.size() * 0.95] / 1e9
                   << " seconds\n"
                   << "99th percentile transaction latency: "
                   << elapsedTime[elapsedTime.size() * 0.99] / 1e9 << " seconds"
                   << std::endl;
#define DO_ONE_TXN_(txn_name)                                              \
  do {                                                                     \
    sort(txn_name.begin(), txn_name.end());                                \
    double total_time = 0;                                                 \
    for (auto i : txn_name) {                                              \
      total_time += i;                                                     \
    }                                                                      \
    if (total_time == 0) break;                                            \
    out_measure_fs << ">>> Total number of " #txn_name " processed "       \
                   << txn_name.size() << "\n"                              \
                   << #txn_name "'s throughput "                           \
                   << txn_name.size() / total_time * 1e9 << "\n"           \
                   << #txn_name "'s average transaction latency: "         \
                   << total_time / 1e9 / txn_name.size() << " seconds"     \
                   << "\n"                                                 \
                   << #txn_name "'s Median transaction latency: "          \
                   << txn_name[txn_name.size() * 0.5] / 1e9 << "\n"        \
                   << #txn_name "'s 99th percentile transaction latency: " \
                   << txn_name[txn_name.size() * 0.99] / 1e9 << " seconds" \
                   << std::endl;                                           \
  } while (0);

    PROCESS_TXN_TIME
#undef DO_ONE_TXN_
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
  static std::string xactDir, outDir;
  int idx_;
  std::vector<double> elapsedTime;
  std::vector<double> delivery, new_order, order_status, payment, popular_item,
      related_customer, stock_level, top_balance;
};
std::string SQLDriver::xactDir = "data/xact_files/";
std::string SQLDriver::outDir = "data/output/";

}  // namespace ysql_impl
#endif