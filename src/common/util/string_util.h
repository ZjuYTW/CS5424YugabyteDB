#ifndef YDB_PERF_STRING_UTIL_H_
#define YDB_PERF_STRING_UTIL_H_
#include <string>
#include <vector>

namespace ydb_util {
// TODO(ZjuYTW): Refine it to string_view
std::vector<std::string> str_split(const std::string& input,
                                   char delimiter) noexcept;

std::string format(const char* fmt, ...) noexcept;
std::string format(const char* fmt, va_list args) noexcept;
char* getLocalTimeString() noexcept;
}  // namespace ydb_util

#endif