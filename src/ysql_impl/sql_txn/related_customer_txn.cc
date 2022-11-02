#include "ysql_impl/sql_txn/related_customer_txn.h"

#include <pqxx/pqxx>

namespace ydb_util {
Status YSQLRelatedCustomerTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Related-Customer Transaction started";
  auto InputString = format("R %d %d %d", c_w_id_, c_d_id_, c_id_);
  auto start = std::chrono::system_clock::now();
  int retryCount = 0;

  while (retryCount < MAX_RETRY_COUNT) {
    try {
      pqxx::work txn(*conn_);
      std::string isolationSQL =
          "begin transaction isolation level serializable read only "
          "deferrable;";
      txn.exec(isolationSQL);
      txn.exec(format("set yb_transaction_priority_lower_bound = %f",
                      retryCount * 0.2));
      outputs.push_back(
          format("Customer identifier: (%d, %d, %d)", c_w_id_, c_d_id_, c_id_));
      pqxx::result orders = getOrdersSQL_(c_w_id_, c_d_id_, c_id_, &txn);
      std::unordered_set<std::string> customers;
      for (auto order : orders) {
        pqxx::result orderLines =
            getOrderLineSQL_(order["o_id"].c_str(), order["o_d_id"].c_str(),
                             order["o_w_id"].c_str(), &txn);
        std::vector<int> orderLineIds;
        for (auto orderLine : orderLines) {
          orderLineIds.push_back(orderLine["ol_i_id"].as<int>());
        }
        addCustomerSQL_(c_w_id_, orderLineIds, customers, &txn);
      }
      txn.commit();
      LOG_INFO << "Found " << customers.size() << " related customers";

      if (customers.size() != 0) {
        outputs.push_back("(a) Customer Identifiers:");
        for (auto customer : customers) {
          outputs.push_back(customer);
        }
      } else {
        outputs.push_back("(a) No customer found");
      }

      txn_out_ << InputString << std::endl;
      for (auto& output : outputs) {
        txn_out_ << output << std::endl;
      }
      auto end = std::chrono::system_clock::now();
      *diff_t = (end - start).count();
      return Status::OK();
    } catch (const std::exception& e) {
      retryCount++;
      LOG_ERROR << e.what();
      if (retryCount == MAX_RETRY_COUNT) {
        err_out_ << InputString << std::endl;
        err_out_ << e.what() << "\n";
      }
      // if Failed, Wait for 100 ms to try again
      LOG_INFO << "Related Customers Retry time:" << retryCount
               << "ERROR MESSAGE: " << e.what();
      int randRetryTime = rand() % 100 + 1;
      std::this_thread::sleep_for(
          std::chrono::milliseconds((100 + randRetryTime) * retryCount));
    }
  }
  return Status::Invalid("retry times exceeded max retry count");
}

void YSQLRelatedCustomerTxn::addCustomerSQL_(
    int w_id, std::vector<int> items,
    std::unordered_set<std::string>& customers, pqxx::work* txn) {
  std::string itemsStr = "";
  for (auto item : items) {
    itemsStr += std::to_string(item) + ",";
  }
  itemsStr.pop_back();

  std::string query = format(
      "SELECT ol_w_id, ol_d_id, ol_o_id "
      "FROM orderline "
      "WHERE ol_i_id IN (%s) "
      "AND ol_w_id != (%s) "
      "GROUP BY (ol_w_id, ol_d_id, ol_o_id) "
      "HAVING COUNT(*) >= 2",
      itemsStr.c_str(), std::to_string(w_id).c_str());
  pqxx::result result = txn->exec(query);
  if (result.empty()) {
    return;
  }
  for (auto row : result) {
    int ol_w_id = row["ol_w_id"].as<int>();
    int ol_d_id = row["ol_d_id"].as<int>();
    int ol_o_id = row["ol_o_id"].as<int>();
    int c_id = getCustomerIdSQL_(row["ol_w_id"].c_str(), row["ol_d_id"].c_str(),
                                 row["ol_o_id"].c_str(), txn);
    std::string customer = "(" + std::to_string(ol_w_id) + ", " +
                           std::to_string(ol_d_id) + ", " +
                           std::to_string(c_id) + ")";
    if (customers.find(customer) == customers.end()) {
      customers.insert(customer);
    }
  }
}

pqxx::result YSQLRelatedCustomerTxn::getOrdersSQL_(int c_w_id, int c_d_id,
                                                   int c_id, pqxx::work* txn) {
  std::string getOrdersSQL =
      "SELECT o_id, o_w_id, o_d_id FROM orders WHERE o_w_id = " +
      std::to_string(c_w_id) + " AND o_d_id = " + std::to_string(c_d_id) +
      " AND o_c_id = " + std::to_string(c_id) + ";";
  return txn->exec(getOrdersSQL);
}

pqxx::result YSQLRelatedCustomerTxn::getOrderLineSQL_(std::string o_id,
                                                      std::string o_d_id,
                                                      std::string o_w_id,
                                                      pqxx::work* txn) {
  std::string getOrderLineSQL =
      "SELECT ol_i_id FROM orderline WHERE ol_w_id = " + o_w_id +
      " AND ol_d_id = " + o_d_id + " AND ol_o_id = " + o_id + ";";
  return txn->exec(getOrderLineSQL);
}

int YSQLRelatedCustomerTxn::getCustomerIdSQL_(std::string w_id,
                                              std::string d_id,
                                              std::string o_id,
                                              pqxx::work* txn) {
  std::string getCustomerIdSQL =
      "SELECT o_c_id FROM orders WHERE o_w_id = " + w_id +
      " AND o_d_id = " + d_id + " AND o_id = " + o_id + ";";
  pqxx::result res = txn->exec(getCustomerIdSQL);
  if (res.empty()) {
    throw std::string("Customer not found");
  }
  return res[0]["o_c_id"].as<int>();
}
}  // namespace ydb_util