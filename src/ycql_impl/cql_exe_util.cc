#include "ycql_impl/cql_exe_util.h"
#include <thread>

namespace ycql_impl {
bool ValidOrSleep(bool done) noexcept {
  if (!done) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return done;
}
}  // namespace ycql_impl
