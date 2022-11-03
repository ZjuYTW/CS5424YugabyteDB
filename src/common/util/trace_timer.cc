#include "common/util/trace_timer.h"

#include <chrono>

namespace ydb_util {
int64_t GetNowMicroSec() noexcept {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}
}  // namespace ydb_util