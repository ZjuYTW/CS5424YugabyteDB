#ifndef YDB_PERF_COMMON_STATUS_H_
#define YDB_PERF_COMMON_STATUS_H_
#include <string>

namespace ydb_util {
enum class StatusCode : unsigned char {
  kOK = 0,
  kInvalid = 1,
  kTypeError = 2,
  kIOError = 3,
  kNotImplemented = 4,
  kAssertionFailed = 5,
  kEndOfFile = 6,
  kConnectionFailed = 7,

  kUnknownError = 255,
};

/**
 *
 * @brief  A helper general return type. We could add the return status when
 * there is a need.
 *
 */
class Status {
 public:
  Status() noexcept : state_(nullptr) {}
  Status(StatusCode, const std::string&) noexcept;
  ~Status() noexcept { DeleteState(); }

  // Move semantic
  inline Status(Status&& s) noexcept;
  inline Status& operator=(Status&& s) noexcept;

  // Copy semantic
  inline Status(const Status& s) noexcept;
  inline Status& operator=(const Status& s) noexcept;

  static Status OK() noexcept { return Status(); }

  static Status Invalid() noexcept { return Status(StatusCode::kInvalid, ""); }
  static Status Invalid(const std::string& msg) noexcept {
    return Status(StatusCode::kInvalid, msg);
  }

  // return an error status for type errors (such as mismatching data types).
  static Status TypeError() noexcept {
    return Status(StatusCode::kTypeError, "");
  }

  /// Return an error status for IO errors (e.g. Failed to open or read from a
  /// file).
  static Status IOError(const std::string& msg = "") noexcept {
    return Status(StatusCode::kIOError, msg);
  }

  static Status NotImplemented(const std::string& msg = "") noexcept {
    return Status(StatusCode::kNotImplemented, msg);
  }

  static Status AssertionFailed(const std::string& msg = "") noexcept {
    return Status(StatusCode::kAssertionFailed, msg);
  }

  static Status EndOfFile(const std::string& msg = "") noexcept {
    return Status(StatusCode::kEndOfFile, msg);
  }

  static Status ConnectionFailed(const std::string& msg = "") noexcept {
    return Status(StatusCode::kConnectionFailed, msg);
  }

  static Status UnknownError(const std::string& msg = "") noexcept {
    return Status(StatusCode::kUnknownError, msg);
  }

  bool ok() const noexcept { return state_ == nullptr; }

  bool isInvalid() const noexcept { return code() == StatusCode::kInvalid; }

  bool isTypeError() const noexcept { return code() == StatusCode::kTypeError; }

  bool isIOError() const noexcept { return code() == StatusCode::kIOError; }

  bool isNotImplemented() const noexcept {
    return code() == StatusCode::kNotImplemented;
  }

  bool isAssertionFailed() const noexcept {
    return code() == StatusCode::kAssertionFailed;
  }

  bool isEndOfFile() const noexcept { return code() == StatusCode::kEndOfFile; }

  bool isConnectionFailed() const noexcept {
    return code() == StatusCode::kConnectionFailed;
  }

  bool isUnknownError() const noexcept {
    return code() == StatusCode::kUnknownError;
  }

  StatusCode code() const noexcept {
    return ok() ? StatusCode::kOK : state_->code;
  }

  std::string ToString() const noexcept;

  std::string CodeAsString() const noexcept;

 private:
  struct State {
    StatusCode code;
    std::string msg;
  };
  State* state_;

  void DeleteState() {
    delete state_;
    state_ = nullptr;
  }
};

Status::Status(const Status& s) noexcept
    : state_((s.state_ == nullptr) ? nullptr : new State(*s.state_)) {}

Status::Status(Status&& s) noexcept : state_(s.state_) { s.state_ = nullptr; }

Status& Status::operator=(Status&& s) noexcept {
  delete state_;
  state_ = s.state_;
  s.state_ = nullptr;
  return *this;
}

Status& Status::operator=(const Status& s) noexcept {
  if (state_ != s.state_) {
    delete state_;
    if (s.state_) {
      state_ = new State(*s.state_);
    } else {
      state_ = nullptr;
    }
  }
  return *this;
}

}  // namespace ydb_util
#endif