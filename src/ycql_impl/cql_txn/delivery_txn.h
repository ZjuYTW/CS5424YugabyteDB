#ifndef YCQL_IMPL_DELIVERY_TXN_H_
#define YCQL_IMPL_DELIVERY_TXN_H_
#include "common/txn/delivery_txn.h"

namespace ycql_impl {
class YCQLDeliveryTxn : public ydb_util::DeliveryTxn {
  using Status = ydb_util::Status;

 public:
  YCQLDeliveryTxn(CassSession* session, std::ofstream& txn_out,
                  std::ofstream& err_out)
      : DeliveryTxn(), conn_(session), txn_out_(txn_out), err_out_(err_out) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  FRIEND_TEST(TxnArgsParserTest, delivery);
  CassSession* conn_;
  std::ofstream& txn_out_;
  std::ofstream& err_out_;
};
};  // namespace ycql_impl

#endif