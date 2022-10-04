#ifndef YDB_PERF_STRING_UTIL_H_
#define YDB_PERF_STRING_UTIL_H_
#include <string>
#include <vector>

namespace ydb_util {
// TODO(ZjuYTW): Refine it to string_view
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
}  // namespace ydb_util

#endif