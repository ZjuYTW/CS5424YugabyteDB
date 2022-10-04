#include <iostream>

#include "cassandra.h"
#include "common/parser/parser.h"
#include "ThreadPool.h"

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

// Create a new cluster.
CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

// Connect to the cluster given a session.
CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError execute_and_log_select(CassSession* session, const char* stmt) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(stmt, 0);

  future = cass_session_execute(session, statement);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);
    if (cass_iterator_next(iterator)) {
      const CassRow* row = cass_iterator_get_row(iterator);
      int age;
      const char* name;
      size_t name_length;
      const char* language;
      size_t language_length;
      cass_value_get_string(cass_row_get_column(row, 0), &name, &name_length);
      cass_value_get_int32(cass_row_get_column(row, 1), &age);
      cass_value_get_string(cass_row_get_column(row, 2), &language,
                            &language_length);
      printf("Select statement returned: Row[%.*s, %d, %.*s]\n",
             (int)name_length, name, age, (int)language_length, language);
    } else {
      printf("Unable to fetch row!\n");
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main() {
  // Ensure you log errors.
  cass_log_set_level(CASS_LOG_ERROR);

  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  CassFuture* close_future = NULL;
  char* hosts = "192.168.48.244";

  cluster = create_cluster(hosts);

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  CassError rc = CASS_OK;
  rc = execute_query(session, "CREATE KEYSPACE IF NOT EXISTS ybdemo");
  if (rc != CASS_OK) return -1;
  printf("Created keyspace ybdemo\n");

  rc = execute_query(session, "DROP TABLE IF EXISTS ybdemo.employee");
  if (rc != CASS_OK) return -1;

  rc = execute_query(session,
                     "CREATE TABLE ybdemo.employee (id int PRIMARY KEY, \
                                              name varchar, \
                                              age int, \
                                              language varchar)");
  if (rc != CASS_OK) return -1;
  printf("Created table ybdemo.employee\n");

  const char* insert_stmt =
      "INSERT INTO ybdemo.employee (id, name, age, language) VALUES (1, "
      "'John', 35, 'C/C++')";
  rc = execute_query(session, insert_stmt);
  if (rc != CASS_OK) return -1;
  printf("Inserted data: %s\n", insert_stmt);

  const char* select_stmt =
      "SELECT name, age, language from ybdemo.employee WHERE id = 1";
  rc = execute_and_log_select(session, select_stmt);
  if (rc != CASS_OK) return -1;

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}