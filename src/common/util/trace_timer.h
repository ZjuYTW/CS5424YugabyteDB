#ifndef YDB_PERF_TRACE_TIMER_H_
#define YDB_PERF_TRACE_TIMER_H_
#include <stdarg.h>

#include <cassert>
#include <cstdio>
#include <stack>
#include <string>

#include "common/util/logger.h"
#include "common/util/string_util.h"

#ifndef TRACE_GUARD
#define TRACE_GUARD__(line)                                                 \
  do {                                                                      \
    if (nullptr == trace_timer_) {                                          \
      break;                                                                \
    }                                                                       \
    std::string file_##line(__FILE__);                                      \
    auto last_slash_pos_##line = file_##line.find_last_of('/');             \
    if (last_slash_pos_##line != std::string::npos) {                       \
      last_slash_pos_##line++;                                              \
    } else {                                                                \
      last_slash_pos_##line = 0;                                            \
    }                                                                       \
    auto suffix_filename_##line =                                           \
        file_##line.substr(last_slash_pos_##line, file_##line.size());      \
    trace_timer_->StartSpan("%.*s:%ld,%s", suffix_filename_##line.size(),   \
                            suffix_filename_##line.data(), __LINE__,        \
                            __func__);                                      \
  } while (0);                                                              \
  auto defer_finish_span_##line = ydb_util::defer(                          \
      [trace_timer_ = this->trace_timer_] { trace_timer_->FinishSpan(); }); \
  if (nullptr == trace_timer_) {                                            \
    defer_finish_span_##line.Cancel();                                      \
  }

#define TRACE_GUARD TRACE_GUARD__(__LINE__)

#endif

namespace ydb_util {
int64_t GetNowMicroSec() noexcept;

class TraceTimer {
  struct Span {
    int64_t start_ts_us{0};
    std::string name;
  };

 public:
  void Reset() noexcept {
    int64_t ts_us = GetNowMicroSec();
    start_us_ = ts_us;
    pos_ = 0;
  }

  void Print() noexcept {
    LOG_DEBUG << format("start_us = %ld total_timeu=%ld, trace log: %.*s",
                        start_us_, GetNowMicroSec() - start_us_, pos_, buffer_);
  }

  void StartSpan(const char* fmt, ...) noexcept {
    va_list args;
    va_start(args, fmt);
    auto name = format(fmt, args);
    va_end(args);
    Span span = {.start_ts_us = GetNowMicroSec(), .name = name};
    span_stk_.push(std::move(span));
    AppendLog("-->");
  }

  void FinishSpan() noexcept {
    assert(!span_stk_.empty());
    auto& span = span_stk_.top();
    AppendLog("<--, span_exec_time = %ldus\n",
              GetNowMicroSec() - span.start_ts_us);
    span_stk_.pop();
  }

  void AppendLog(const char* fmt, ...) noexcept {
    va_list args;
    va_start(args, fmt);
    PrintSpanInfo_();
    AppendLog_(fmt, args);
  }

 private:
  void AppendLog_(const char* fmt, ...) noexcept {
    va_list args;
    va_start(args, fmt);
    AppendLog_(fmt, args);
    va_end(args);
  }

  void AppendLog_(const char* fmt, va_list args) noexcept {
    int len = vsnprintf(&buffer_[pos_], kTraceLogBufferLen - pos_, fmt, args);
    assert(len >= 0);
    // to handle more easily, expect buffer won't flow
    pos_ += len;
  }

  void PrintSpanInfo_() noexcept {
    auto cur_time = GetNowMicroSec();
    if (span_stk_.empty()) {
      AppendLog_("step=%ldus total=%ldus \n", cur_time - last_step_us_,
                 cur_time - start_us_);
    } else {
      auto& span = span_stk_.top();
      AppendLog_("span=%s step=%ldus total=%ldus \n", span.name.c_str(),
                 cur_time - last_step_us_, cur_time - start_us_);
    }
    last_step_us_ = cur_time;
  }

 private:
  static constexpr size_t kTraceLogBufferLen = 8192 * 2;
  std::stack<Span> span_stk_;
  char buffer_[kTraceLogBufferLen]{"\0"};
  size_t pos_{0};
  int64_t start_us_;
  int64_t last_step_us_;
};
};  // namespace ydb_util

#endif