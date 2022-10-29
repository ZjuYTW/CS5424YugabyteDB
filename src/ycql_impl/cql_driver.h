#ifndef YDB_PERF_CQL_DRIVER_H_
#define YDB_PERF_CQL_DRIVER_H_
#include <memory>
#include <string>

#include "cassandra.h"
#include "common/txn/txn_type.h"
#include "common/util/status.h"
#include "ycql_impl/ycql_parser.h"

namespace ycql_impl {
class CQLDriver {
  using Status = ydb_util::Status;
  using Txn = ydb_util::Txn;

 public:
  CQLDriver(const std::string& ip, const CassCluster* cluster, int idx)
      : ip_add_(ip) {
    idx_ = idx;
    cluster_ = cluster;
  }

  Status operator()() {
    std::string filename = xactDir + std::to_string(idx_) + ".txt";
    std::string outputMeasure =
        outDir + "/measure_log/cql_" + std::to_string(idx_) + ".out";
    std::string outputTxn =
        outDir + "/txn_log/sql_" + std::to_string(idx_) + ".out";
    std::string outputErr =
        outDir + "/err_log/sql_" + std::to_string(idx_) + ".out";

    auto out_txn_fs = std::ofstream(outputTxn, std::ios::out);
    auto out_measure_fs = std::ofstream(outputMeasure, std::ios::out);
    auto out_err_fs = std::ofstream(outputErr, std::ios::out);

    CassSession* session = cass_session_new();
    if (connect_session(session, cluster_) != CASS_OK) {
      cass_session_free(session);
      return Status::ConnectionFailed();
    }
    std::unique_ptr<ydb_util::Parser> parser_p =
        std::make_unique<YCQLParser>(filename, session, out_txn_fs, out_err_fs);

    auto s = parser_p->Init();
    if (!s.ok()) {
      cass_session_free(session);
      return s;
    }
    std::vector<double> elapsedTime;
    while (true) {
      // here we use smart ptr to avoid delete manully
      std::unique_ptr<Txn> t = nullptr;
      s = parser_p->GetNextTxn(&t);
      if (!s.ok()) {
        // EndOfFile or Somethin Bad
        break;
      }
      double processTime;
      s = t->Execute(&processTime);
      if (!s.ok()) {
        LOG_ERROR << "CQL Transaction failed";
        break;
      } else {
        elapsedTime.push_back(processTime);
      }
    }

    sort(elapsedTime.begin(), elapsedTime.end());
    double totalTime = 0;
    for (auto i : elapsedTime) {
      totalTime += i;
    }

    out_measure_fs << "Total number of transactions processed: "
                   << elapsedTime.size() << "\n"
                   << "Total elapsed time for processing the transactions: "
                   << totalTime << "\n"
                   << "Transaction throughput: "
                   << elapsedTime.size() / totalTime << "\n"
                   << "Average transaction latency: "
                   << totalTime * 60 / elapsedTime.size() << "\n"
                   << "Median transaction latency: "
                   << elapsedTime[elapsedTime.size() * 0.5] * 60 << "\n"
                   << "95th percentile transaction latency: "
                   << elapsedTime[elapsedTime.size() * 0.95] * 60 << "\n"
                   << "99th percentile transaction latency: "
                   << elapsedTime[elapsedTime.size() * 0.99] * 60 << std::endl;

    cass_session_free(session);
    return s.isEndOfFile() ? Status::OK() : s;
  }

  CassError connect_session(CassSession* session, const CassCluster* cluster) {
    CassError rc = CASS_OK;
    CassFuture* future = cass_session_connect(session, cluster);

    cass_future_wait(future);
    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
      print_error(future);
    }
    cass_future_free(future);
    return rc;
  }

  void print_error(CassFuture* future) {
    const char* message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
  }

 private:
  const CassCluster* cluster_;
  std::string ip_add_;
  static std::string xactDir;
  static std::string outDir;
  int idx_;
};

std::string CQLDriver::xactDir = "data/xact_files/";
std::string CQLDriver::outDir = "data/output/";
};  // namespace ycql_impl

#endif