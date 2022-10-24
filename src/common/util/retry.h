#include <functional>

#include "status.h"
#include "ycql_impl/cql_exe_util.h"

using ydb_util::Status;
Status Retry(std::function<Status()> func, size_t max_attempts,
             unsigned long retry_interval_usecs);