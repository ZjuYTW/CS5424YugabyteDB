#include "ycql_impl/cql_exe_util.h"

#include <thread>

namespace ycql_impl {
bool ValidOrSleep(bool done) noexcept {
  if (!done) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return done;
}

Status Retry(const std::function<ydb_util::Status()>& func,
             size_t max_attempts) {
  Status st;
  int attempt = 0;
  do {
    st = func();
    if (st.ok()) return st;
    ycql_impl::ValidOrSleep(false);
  } while (attempt++ < max_attempts);
  return st;
}
}  // namespace ycql_impl
