#include <string>
#include <vector>

#include "cassandra.h"
#include "common/util/defer.h"
#include "common/util/status.h"

namespace ycql_impl {
template <size_t idx>
CassError cql_statment_fill_args(CassStatement* statement) noexcept {
  return CassError::CASS_OK;
}

template <size_t idx = 0, typename T, typename... Args>
CassError cql_statment_fill_args(CassStatement* statement, T first,
                                 Args... args) noexcept {
  CassError rc = CASS_OK;
  if constexpr (std::is_same_v<bool, T>) {
    rc = cass_statement_bind_bool(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statment_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<uint32_t, T>) {
    rc = cass_statement_bind_uint32(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statment_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<uint64_t, T>) {
    rc = cass_statement_bind_uint64(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statment_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<double, T>) {
    rc = cass_statement_bind_double(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statment_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<int, T>) {
    rc = cass_statement_bind_int32(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statment_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<const char*, T> || std::is_same_v<char*, T>) {
    rc = cass_statement_bind_string(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statment_fill_args<idx + 1>(statement, args...);
  }
  // TODO(ZjuYTW): Add more type if needed

  // unreachable here
  assert(false);
  return CassError::CASS_ERROR_LIB_INVALID_STATE;
}

template <typename... Args>
ydb_util::Status execute_read_cql(CassSession* session, const std::string& stmt,
                                  CassIterator** iterator,
                                  Args... args) noexcept {
  CassError rc = CASS_OK;
  auto st = ydb_util::Status::OK();
  CassFuture* future = nullptr;
  auto size = sizeof...(Args);
  CassStatement* statement = cass_statement_new(stmt.c_str(), size);
  auto ret_func = [&statement, &future]() {
    cass_statement_free(statement);
    if (future) {
      cass_future_free(future);
    }
  };
  DEFER(std::move(ret_func));

  rc = cql_statment_fill_args(statement, args...);
  if (rc != CASS_OK) {
    st = ydb_util::Status::ExecutionFailed(cass_error_desc(rc));
    return st;
  }
  future = cass_session_execute(session, statement);
  // For "Read", we use cass_future_get_result
  auto result = cass_future_get_result(future);
  if (result == nullptr) {
    // An error occur
    rc = cass_future_error_code(future);
    st = ydb_util::Status::ExecutionFailed(cass_error_desc(rc));
    return st;
  }
  // Else we process the result
  *iterator = cass_iterator_from_result(result);
  return st;
}

template <typename... Args>
ydb_util::Status execute_write_cql(CassSession* session,
                                   const std::string& stmt,
                                   Args... args) noexcept {
  CassError rc = CASS_OK;
  auto st = ydb_util::Status::OK();
  CassFuture* future = nullptr;
  auto size = sizeof...(Args);
  CassStatement* statement = cass_statement_new(stmt.c_str(), size);
  auto ret_func = [&statement, &future]() {
    cass_statement_free(statement);
    if (future) {
      cass_future_free(future);
    }
  };
  DEFER(std::move(ret_func));

  rc = cql_statment_fill_args(statement, args...);
  if (rc != CASS_OK) {
    st = ydb_util::Status::ExecutionFailed(cass_error_desc(rc));
    return st;
  }
  future = cass_session_execute(session, statement);
  // For "Write", we use cass_future_error_code
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    st = ydb_util::Status::ExecutionFailed(cass_error_desc(rc));
  }
  return st;
}

template <typename T>
T GetValueFromCassRow(CassIterator* it, const char* col_name) noexcept {
  T ret;
  auto rc = CASS_OK;
  auto row = cass_iterator_get_row(it);
  if constexpr (std::is_same_v<int64_t, T>) {
    rc = cass_value_get_int64(cass_row_get_column_by_name(row, col_name), &ret);
    assert(rc == CASS_OK);
  } else if constexpr (std::is_same_v<uint32_t, T>) {
    rc =
        cass_value_get_uint32(cass_row_get_column_by_name(row, col_name), &ret);
    assert(rc == CASS_OK);
  } else if constexpr (std::is_same_v<double, T>) {
    rc =
        cass_value_get_double(cass_row_get_column_by_name(row, col_name), &ret);
    assert(rc == CASS_OK);
  } else if constexpr (std::is_same_v<int32_t, T>) {
    rc = cass_value_get_int32(cass_row_get_column_by_name(row, col_name), &ret);
    assert(rc == CASS_OK);
  } else if constexpr (std::is_same_v<std::string, T>) {
    const char* buf;
    size_t sz = 0;
    rc = cass_value_get_string(cass_row_get_column_by_name(row, col_name), &buf,
                               &sz);
    assert(rc == CASS_OK);
    return std::string(buf);
  } else {
    assert(false);
  }
  return ret;
}
}  // namespace ycql_impl