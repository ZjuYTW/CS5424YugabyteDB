#ifndef YDB_PERF_COMMON_LOGGER_H_
#define YDB_PERF_COMMON_LOGGER_H_

#include <iostream>
#include <map>
#include <sstream>
#include <mutex>

// separate release log and debug log for benchmarking
// thread-safe Logger
#define LOG(level)                                          \
  ydb_util::Logger::LoggerStream(level, __FILE__, __LINE__, \
                                 ydb_util::ydb_logger)
#define LOG_INFO LOG(ydb_util::LogLevel::INFO)
#define LOG_WARNING LOG(ydb_util::LogLevel::WARNING)
#define LOG_DEBUG LOG(ydb_util::LogLevel::DEBUG)
#define LOG_ERROR LOG(ydb_util::LogLevel::ERROR)
#define LOG_FATAL LOG(ydb_util::LogLevel::FATAL)
#define SET_LEVEL(level) ydb_util::ydb_logger.set_level(level)

namespace ydb_util {

class LogLevel {
 public:
  static const int INFO;
  static const int WARNING;
  static const int DEBUG;
  static const int ERROR;
  static const int FATAL;
};

class Logger {
 public:
  Logger() : LOG_LEVEL(LogLevel::INFO) {}
  class LoggerStream : public std::ostringstream {
   public:
    LoggerStream(int level, const char *file_name, int line_no,
                 Logger &raw_log);
    ~LoggerStream() noexcept override;

   private:
    int line_no, level;
    Logger &raw_log;
  };
  inline void set_level(int level) { this->LOG_LEVEL = level; }
  int LOG_LEVEL;
  std::mutex m_mutex;
};

static Logger ydb_logger;

}  // namespace ydb_util

#endif  // YDB_PERF_COMMON_LOGGER_H_
