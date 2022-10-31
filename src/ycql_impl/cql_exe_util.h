#ifndef YCQL_IMPL_EXE_UTIL_H_
#define YCQL_IMPL_EXE_UTIL_H_

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "cassandra.h"
#include "common/util/defer.h"
#include "common/util/logger.h"
#include "common/util/status.h"
#include "ycql_impl/defines.h"

namespace ycql_impl {
template <size_t idx = 0>
CassError cql_statement_fill_args(CassStatement* statement) noexcept {
  return CassError::CASS_OK;
}

template <size_t idx = 0, typename T, typename... Args>
CassError cql_statement_fill_args(CassStatement* statement, T first,
                                  Args... args) noexcept {
  CassError rc = CASS_OK;
  if constexpr (std::is_same_v<bool, T>) {
    rc = cass_statement_bind_bool(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statement_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<uint32_t, T>) {
    rc = cass_statement_bind_uint32(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statement_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<int64_t, T>) {
    rc = cass_statement_bind_int64(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statement_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<double, T>) {
    rc = cass_statement_bind_double(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statement_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<int32_t, T>) {
    rc = cass_statement_bind_int32(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statement_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<const char*, T> || std::is_same_v<char*, T>) {
    rc = cass_statement_bind_string(statement, idx, first);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statement_fill_args<idx + 1>(statement, args...);
  }
  if constexpr (std::is_same_v<std::vector<int32_t>, T>) {
    auto col = cass_collection_new(CASS_COLLECTION_TYPE_LIST, first.size());
    for (auto& ele : first) {
      cass_collection_append_int32(col, ele);
    }
    rc = cass_statement_bind_collection(statement, idx, col);
    cass_collection_free(col);
    if (rc != CASS_OK) {
      return rc;
    }
    return cql_statement_fill_args<idx + 1>(statement, args...);
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

  rc = cql_statement_fill_args(statement, args...);
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

  rc = cql_statement_fill_args(statement, args...);
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
std::optional<T> GetValueFromCassRow(CassIterator* it,
                                     const char* col_name) noexcept {
  T ret;
  auto rc = CASS_OK;
  auto row = cass_iterator_get_row(it);
  if constexpr (std::is_same_v<int64_t, T>) {
    rc = cass_value_get_int64(cass_row_get_column_by_name(row, col_name), &ret);
    if (rc == CassError::CASS_ERROR_LIB_NULL_VALUE) {
      return std::nullopt;
    }
    assert(rc == CASS_OK);
  } else if constexpr (std::is_same_v<uint32_t, T>) {
    rc =
        cass_value_get_uint32(cass_row_get_column_by_name(row, col_name), &ret);
    if (rc == CassError::CASS_ERROR_LIB_NULL_VALUE) {
      return std::nullopt;
    }
    assert(rc == CASS_OK);
  } else if constexpr (std::is_same_v<double, T>) {
    rc =
        cass_value_get_double(cass_row_get_column_by_name(row, col_name), &ret);
    if (rc == CassError::CASS_ERROR_LIB_NULL_VALUE) {
      return std::nullopt;
    }
    assert(rc == CASS_OK);
  } else if constexpr (std::is_same_v<int32_t, T>) {
    rc = cass_value_get_int32(cass_row_get_column_by_name(row, col_name), &ret);
    if (rc == CassError::CASS_ERROR_LIB_NULL_VALUE) {
      return std::nullopt;
    }
    assert(rc == CASS_OK);
  } else if constexpr (std::is_same_v<std::string, T>) {
    const char* buf;
    size_t sz = 0;
    rc = cass_value_get_string(cass_row_get_column_by_name(row, col_name), &buf,
                               &sz);
    if (rc == CassError::CASS_ERROR_LIB_NULL_VALUE) {
      return std::nullopt;
    }
    assert(rc == CASS_OK);
    return std::string(buf, sz);
  } else if constexpr (std::is_same_v<std::vector<int32_t>, T>) {
    auto tmp_it = cass_iterator_from_collection(
        cass_row_get_column_by_name(row, col_name));
    assert(tmp_it != nullptr);
    while (cass_iterator_next(tmp_it)) {
      int32_t output;
      rc = cass_value_get_int32(cass_iterator_get_value(tmp_it), &output);
      assert(rc == CASS_OK);
      ret.push_back(output);
    }
    cass_iterator_free(tmp_it);
  } else {
    assert(false);
  }
  return ret;
}

bool ValidOrSleep(bool done) noexcept;

ydb_util::Status Retry(const std::function<ydb_util::Status()>& func,
                       size_t max_attempts);

double GetDTax(CassIterator* district_it) noexcept;

double GetWTax(CassIterator* warehouse_it) noexcept;

double GetDiscount(CassIterator* custom_it) noexcept;

ydb_util::Status BatchExecute(const std::vector<CassStatement*>& stmts,
                              CassSession* conn) noexcept;

// Note: @param base is to convert enlarged number back to origin value, if base
// is 0 then we don't do the convert
template <typename T>
std::string GetStringValue(const std::optional<T>& opt_val,
                           int base = 0) noexcept {
  if (!opt_val.has_value()) {
    return "null";
  }
  if constexpr (std::is_same_v<std::string, T>) {
    return opt_val.value();
  } else {
    if (base == 0) {
      return std::to_string(opt_val.value());
    } else {
      return std::to_string(static_cast<double>(opt_val.value()) / base);
    }
  }
}

}  // namespace ycql_impl

#endif