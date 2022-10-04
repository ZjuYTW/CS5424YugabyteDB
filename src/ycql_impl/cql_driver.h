#ifndef YDB_PERF_CQL_DRIVER_H_
#define YDB_PERF_CQL_DRIVER_H_
#include <string>

#include "cassandra.h"
#include "common/parser/parser.h"
#include "common/txn/txn_type.h"
#include "common/util/status.h"

namespace ycql_impl {
class CQLDriver {
  using Status = ydb_util::Status;
  using Txn = ydb_util::Txn<CassSession>;

 public:
  CQLDriver(const std::string& ip, const CassCluster* cluster, int idx)
      : ip_add_(ip) {
    idx = idx_;
    cluster_ = cluster;
  }

  Status operator()() {
    std::string filename = xactDir + std::to_string(idx_) + ".txt";
    CassSession* session = cass_session_new();
    if (connect_session(session, cluster_) != CASS_OK) {
      cass_session_free(session);
      return Status::ConnectionFailed();
    }
    ydb_util::Parser<CassSession> parser(filename, session);
    auto s = parser.Init();
    if (!s.ok()) {
      return s;
    }
    while (true) {
      Txn* t = nullptr;
      if (parser.GetNextTxn(&t).isEndOfFile()) {
        break;
      }
      t->ExecuteCQL();
    }
    cass_session_free(session);
    return Status::OK();
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
  std::string xactDir = "data/xact-files/";
  int idx_;
};

};  // namespace ycql_impl

#endif