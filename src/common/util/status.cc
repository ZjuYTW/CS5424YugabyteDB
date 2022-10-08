#include "common/util/status.h"

namespace ydb_util {
Status::Status(StatusCode code, const std::string& msg) noexcept {
  state_ = new State;
  state_->code = code;
  state_->msg = msg;
}

// TODO(ZjuYTW): Change switch into macro
std::string Status::CodeAsString() const noexcept {
  if (state_ == nullptr) {
    return "OK";
  }
  const char* type;
  switch (code()) {
    case StatusCode::kOK:
      type = "OK";
      break;
    case StatusCode::kInvalid:
      type = "Invalid";
      break;
    case StatusCode::kTypeError:
      type = "TypeError";
      break;
    case StatusCode::kIOError:
      type = "IOError";
      break;
    case StatusCode::kNotImplemented:
      type = "NotImplemented";
      break;
    case StatusCode::kAssertionFailed:
      type = "AssertionFailed";
      break;
    case StatusCode::kEndOfFile:
      type = "EndOfFile";
      break;
    case StatusCode::kConnectionFailed:
      type = "ConnectionFailed";
      break;
    case StatusCode::kUnknownError:
      type = "UnknownError";
      break;
    default:
      assert(false);
      break;
  }
  return std::string(type);
}

std::string Status::ToString() const noexcept {
  std::string result(CodeAsString());
  if (state_ == nullptr) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  return result;
}
};  // namespace ydb_util