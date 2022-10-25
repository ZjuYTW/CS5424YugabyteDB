#include <functional>

#include "status.h"

using ydb_util::Status;
Status Retry(std::function<Status()> func, size_t max_attempts);