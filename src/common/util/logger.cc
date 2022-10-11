#include "common/util/logger.h"

namespace ydb_util {

const int LogLevel::INFO = 1;
const int LogLevel::WARNING = 2;
const int LogLevel::DEBUG = 3;
const int LogLevel::ERROR = 4;
const int LogLevel::FATAL = 5;

static std::map<int, std::string> LevelString = {{LogLevel::INFO, "INFO"},
                                                 {LogLevel::WARNING, "WARNING"},
                                                 {LogLevel::DEBUG, "DEBUG"},
                                                 {LogLevel::ERROR, "ERROR"},
                                                 {LogLevel::FATAL, "FATAL"}};

Logger::LoggerStream::LoggerStream(int level, const char *file_name,
                                   int line_no, ydb_util::Logger &raw_log)
    : line_no(line_no), level(level), raw_log(raw_log) {
  std::ostringstream &now = *this;
  now << "[" << file_name << " : " << LevelString[level] << "] ";
}

Logger::LoggerStream::~LoggerStream() noexcept {
  if (level < raw_log.LOG_LEVEL) return;
  std::unique_lock<std::mutex> lock(raw_log.m_mutex);
  std::cout << this->str() << " (" << line_no << ")" << std::endl;
}

}  // namespace ydb_util