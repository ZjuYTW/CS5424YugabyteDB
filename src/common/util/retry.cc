#include "retry.h"

#include "logger.h"
#include "ycql_impl/cql_exe_util.h"

Status Retry(const std::function<ydb_util::Status()>& func,
             size_t max_attempts) {
  Status st;
  int attempt = 0;
  do {
    st = func();
    if (st.ok()) return st;
    ycql_impl::ValidOrSleep(false);
  } while (attempt++ < max_attempts);
  return ydb_util::Status::ExecutionFailed("Exceed max retry attempts");
}