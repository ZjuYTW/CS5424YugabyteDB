#include "env_util.h"

namespace ydb_util {

std::string getenv(const std::string& key,
                   const std::string& default_value) noexcept {
  const char* value = std::getenv(key.c_str());
  return value ? value : default_value;
}
}  // namespace ydb_util
