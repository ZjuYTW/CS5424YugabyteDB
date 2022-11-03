#include <ycql_impl/defines.h>

std::string ycql_impl::YCQLKeyspace = "ybtest";
std::string ycql_impl::TEST_TXN_OUT_PATH = "data/test_output/";
std::string ycql_impl::TEST_ERR_OUT_PATH = "data/test_output/";
bool ycql_impl::YDB_SKIP_DELIVERY = false;
bool ycql_impl::YDB_SKIP_NEW_ORDER = false;
bool ycql_impl::YDB_SKIP_ORDER_STATUS = false;
bool ycql_impl::YDB_SKIP_PAYMENT = false;
bool ycql_impl::YDB_SKIP_POPULAR_ITEM = false;
bool ycql_impl::YDB_SKIP_RELATED_CUSTOMER = false;
bool ycql_impl::YDB_SKIP_STOCK_LEVEL = false;
bool ycql_impl::YDB_SKIP_TOP_BALANCE = false;