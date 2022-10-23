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
    CassSession* session = cass_session_new();
    if (connect_session(session, cluster_) != CASS_OK) {
      cass_session_free(session);
      return Status::ConnectionFailed();
    }
    std::unique_ptr<ydb_util::Parser> parser_p =
        std::make_unique<YCQLParser>(filename, session);
    auto s = parser_p->Init();
    if (!s.ok()) {
      cass_session_free(session);
      return s;
    }
    while (true) {
      // here we use smart ptr to avoid delete manully
      std::unique_ptr<Txn> t = nullptr;
      s = parser_p->GetNextTxn(&t);
      if (!s.ok()) {
        // EndOfFile or Somethin Bad
        break;
      }
      s = t->Execute();
      if (!s.ok()) {
        break;
      }
    }
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
  int idx_;
};

std::string CQLDriver::xactDir = "data/xact_files/";
};  // namespace ycql_impl

#endif