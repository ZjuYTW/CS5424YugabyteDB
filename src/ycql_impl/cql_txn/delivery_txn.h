#ifndef YCQL_IMPL_DELIVERY_TXN_H_
#define YCQL_IMPL_DELIVERY_TXN_H_
#include "common/txn/delivery_txn.h"

namespace ycql_impl {
class YCQLDeliveryTxn : public ydb_util::DeliveryTxn {
  using Status = ydb_util::Status;

 public:
  explicit YCQLDeliveryTxn(CassSession* session)
      : DeliveryTxn(), conn_(session) {}

  Status Execute(double* diff_t) noexcept override;

 private:
  FRIEND_TEST(TxnArgsParserTest, delivery);
  CassSession* conn_;
};
};  // namespace ycql_impl

#endif