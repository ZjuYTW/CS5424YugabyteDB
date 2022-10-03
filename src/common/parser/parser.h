#ifndef YDB_PERF_PARSER_H_
#define YDB_PERF_PARSER_H_

#include <iostream>
#include <string>

#include "common/txn/txn_type.h"
#include "common/util/status.h"

namespace ydb_util {

// Usage: After init the Parser class with input file path
// Call GetNextTxn to get next Txn class.
class Parser {
 public:
  explicit Parser(const std::string& file_name) : file_name_(file_name) {}

  Status Init() noexcept {
    fs_ = std::ifstream(file_name_, std::ios::in);
    if (!fs_.is_open()) {
      return Status::IOError("Fail to open " + file_name_);
    }
    return Status::OK();
  }

  Status GetNextTxn(Txn** txn) noexcept;

 private:
  static Txn* GetTxnPtr_(char c) noexcept;

  std::string file_name_;
  std::ifstream fs_;
};
}  // namespace ydb_util

#endif