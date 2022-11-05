#ifndef YDB_PERF_CQL_DRIVER_H_
#define YDB_PERF_CQL_DRIVER_H_
#include <memory>
#include <string>

#include "cassandra.h"
#include "common/txn/txn_type.h"
#include "common/util/status.h"
#include "common/util/trace_timer.h"
#include "ycql_impl/ycql_parser.h"

namespace ycql_impl {
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
        outDir + "/txn_log/cql_" + std::to_string(idx_) + ".out";
    std::string outputErr =
        outDir + "/err_log/cql_" + std::to_string(idx_) + ".out";

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
    std::vector<double> delivery, new_order, order_status, payment,
        popular_item, related_customer, stock_level, top_balance;
    while (true) {
      // here we use smart ptr to avoid delete manully
      std::unique_ptr<Txn> t = nullptr;
      s = parser_p->GetNextTxn(&t);
      if (!s.ok()) {
        // EndOfFile or Somethin Bad
        break;
      }
      double processTime;
#ifndef NDEBUG
      t->SetTraceTimer(&trace_timer_);
#endif
      s = t->Execute(&processTime);
      if (!s.ok()) {
        LOG_ERROR << "CQL Transaction failed " << s.ToString();
        break;
      } else {
        elapsedTime.push_back(processTime);
      }
      // Process all single txn
#define DO_ONE_TXN_(txn_name)                           \
  case ydb_util::TxnType::txn_name: {                   \
    std::cout << "Pushed into " #txn_name << std::endl; \
    txn_name.push_back(processTime);                    \
    break;                                              \
  }
      INSERT_TXN_TIME
#undef DO_ONE_TXN_
    }

    sort(elapsedTime.begin(), elapsedTime.end());
    double totalTime = 0;
    for (auto i : elapsedTime) {
      totalTime += i;
    }
    assert(totalTime != 0);

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
    out_measure_fs << ">>> Total number of " #txn_name "processed "        \
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
#ifndef NDEBUG
  ydb_util::TraceTimer trace_timer_;
#endif
};  // namespace ycql_impl

std::string CQLDriver::xactDir = "data/xact_files/";
std::string CQLDriver::outDir = "data/output/";
};  // namespace ycql_impl

#endif