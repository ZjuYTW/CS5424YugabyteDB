#ifndef YDB_PERF_DELIVERY_TXN_H_
#define YDB_PERF_DELIVERY_TXN_H_
#include <thread>

#include "common/txn/txn_type.h"

namespace ydb_util {
class DeliveryTxn : public Txn {
 public:
  explicit DeliveryTxn() : Txn(TxnType::delivery) {}

  virtual ~DeliveryTxn() = default;

  virtual Status Execute() noexcept override = 0;

  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 3) {
      return Status::AssertionFailed("Expect DeliveryTxn has 3 args, but got " +
                                     std::to_string(ids.size()));
    }
    // ignore txn identification 'D'
    w_id_ = stoi(ids[1]);
    carrier_id_ = stoi(ids[2]);
    return Status::OK();
  }

 protected:
  uint32_t w_id_, carrier_id_;

 private:
  FRIEND_TEST(TxnArgsParserTest, delivery);
};

}  // namespace ydb_util
#endif