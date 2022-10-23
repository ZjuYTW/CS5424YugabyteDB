#ifndef YDB_PERF_PARSER_H_
#define YDB_PERF_PARSER_H_

#include <iostream>
#include <memory>
#include <string>

#include "common/txn/txn_type.h"
#include "common/util/logger.h"
#include "common/util/status.h"

namespace ydb_util {
// Usage: After init the Parser class with input file path
// Call GetNextTxn to get next Txn class.
class Parser {
 public:
  explicit Parser(const std::string& file_name) : file_name_(file_name) {}
  virtual ~Parser() = default;

  Status Init() noexcept {
    fs_ = std::ifstream(file_name_, std::ios::in);
    if (!fs_.is_open()) {
      return Status::IOError("Fail to open " + file_name_);
    }
    return Status::OK();
  }

  Status GetNextTxn(std::unique_ptr<Txn>* txn) noexcept {
    assert(txn != nullptr);
    if (!fs_.good()) {
      return Status::EndOfFile();
    }
    std::string line;
    getline(fs_, line);
    if (line.empty()) {
      return Status::EndOfFile();
    }
    LOG_INFO << "line: " << line;
    auto txn_ptr = std::unique_ptr<Txn>(GetTxnPtr_(line[0]));
    auto ret = txn_ptr->Init(line, fs_);
    *txn = std::move(txn_ptr);
    return ret;
  }

 protected:
  virtual Txn* GetTxnPtr_(char c) noexcept = 0;

 private:
  std::string file_name_;
  std::ifstream fs_;
};
}  // namespace ydb_util

#endif