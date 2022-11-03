#ifndef YCQL_IMPL_DEFINES_H_
#define YCQL_IMPL_DEFINES_H_
#include <string>
namespace ycql_impl {
extern std::string YCQLKeyspace;
extern std::string TEST_TXN_OUT_PATH;
extern std::string TEST_ERR_OUT_PATH;
extern bool YDB_SKIP_DELIVERY;
extern bool YDB_SKIP_NEW_ORDER;
extern bool YDB_SKIP_ORDER_STATUS;
extern bool YDB_SKIP_PAYMENT;
extern bool YDB_SKIP_POPULAR_ITEM;
extern bool YDB_SKIP_RELATED_CUSTOMER;
extern bool YDB_SKIP_STOCK_LEVEL;
extern bool YDB_SKIP_TOP_BALANCE;
}  // namespace ycql_impl

#endif