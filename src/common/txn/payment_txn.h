#ifndef YDB_PERF_PAYMENT_TXN_H_
#define YDB_PERF_PAYMENT_TXN_H_
#include <thread>

#include "common/txn/txn_type.h"

namespace ydb_util {
class PaymentTxn : public Txn {
 public:
  explicit PaymentTxn() : Txn(TxnType::payment) {}

  virtual ~PaymentTxn() = default;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    LOG_INFO << "Init payment";
    auto ids = str_split(first_line, ',');
    if (ids.size() != 5) {
      return Status::AssertionFailed(
          "Expect Payment has 5 first line args, but got " +
          std::to_string(ids.size()));
    }
    LOG_INFO << "--------payment Init----------";
    w_id_ = std::stoi(ids[1]);
    d_id_ = std::stoi(ids[2]);
    c_id_ = std::stoi(ids[3]);
    payment_ = std::stod(ids[4]);
    return Status::OK();
  }

  virtual Status Execute(double* diff_t) noexcept override = 0;

 protected:
  uint32_t w_id_, d_id_, c_id_;
  double payment_;

 private:
  FRIEND_TEST(TxnArgsParserTest, payment);
};

}  // namespace ydb_util
#endif