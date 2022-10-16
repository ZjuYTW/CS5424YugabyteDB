#include "string_util.h"

#include <stdarg.h>

#include <cassert>

namespace ydb_util {
std::vector<std::string> str_split(const std::string& input,
                                   char delimiter) noexcept {
  size_t prev = 0;
  std::vector<std::string> result;
  for (size_t i = 0; i <= input.size(); i++) {
    if (i == input.size() || input[i] == delimiter) {
      assert(i != prev);
      result.push_back(input.substr(prev, i - prev));
      prev = i + 1;
    }
  }
  return result;
}
std::string format(const char* fmt, ...) noexcept {
  va_list args;
  va_start(args, fmt);
  const auto len = vsnprintf(nullptr, 0, fmt, args);
  va_end(args);
  std::string r;
  r.resize(static_cast<size_t>(len) + 1);
  va_start(args, fmt);
  vsnprintf(&r.front(), len + 1, fmt, args);
  va_end(args);
  r.resize(static_cast<size_t>(len));
  return r;
}
}  // namespace ydb_util