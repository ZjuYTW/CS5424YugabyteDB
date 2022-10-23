#ifndef YDB_PERF_ENV_UTIL_H_
#define YDB_PERF_ENV_UTIL_H_
#include <cstdlib>
#include <string>

namespace ydb_util {
// getenv return the value of the environment variable named by the key.
// It returns the default value if the variable is not present.
std::string getenv(const std::string& key,
                   const std::string& default_value = "") noexcept;
}  // namespace ydb_util

#endif