#include "ysql_impl/sql_txn/related_customer_txn.h"

#include <pqxx/pqxx>

namespace ydb_util {
Status YSQLRelatedCustomerTxn::Execute(double* diff_t) noexcept {
  LOG_INFO << "Related-Customer Transaction started";
  auto InputString = format("R %d %d %d",c_w_id_,c_d_id_,c_id_);
  auto start = std::chrono::system_clock::now();
  int retryCount = 0;

  if(true){
    auto end = std::chrono::system_clock::now();
    *diff_t = (end-start).count();
    return Status::OK();
  }

  while (retryCount < MAX_RETRY_COUNT) {
    pqxx::work txn(*conn_);
    try {
      txn.exec(format("set yb_transaction_priority_lower_bound = %f",retryCount*0.2));
      pqxx::result orders = getOrdersSQL_(c_w_id_, c_d_id_, c_id_, &txn);
      std::unordered_set<std::string> customers;
      for (auto order : orders) {
        LOG_INFO << "Order: " << order[0].as<int>();
        uint32_t o_id_ = order["o_id"].as<uint32_t>();
        pqxx::result orderLines =
            getOrderLineSQL_(c_w_id_, c_d_id_, o_id_, &txn);
        std::vector<int> orderLineIds;
        for (auto orderLine : orderLines) {
          orderLineIds.push_back(orderLine["ol_i_id"].as<int>());
        }
        addCustomerSQL_(c_w_id_, orderLineIds, customers, &txn);
      }
      txn.commit();

      if (customers.size() != 0) {
        outputs.push_back("(a) Customer Identifiers:");
        for (auto customer : customers) {
          outputs.push_back(customer);
        }
      } else {
        outputs.push_back("(a) No customer found");
      }
      
      txn_out_<<InputString<<std::endl;
      for (auto& output : outputs) {
        txn_out_ << output << std::endl;
      }
      auto end = std::chrono::system_clock::now();
      *diff_t = (end-start).count();
      return Status::OK();
    } catch (const std::exception& e) {
      txn.abort();
      retryCount++;
      LOG_ERROR << e.what();
      if (retryCount == MAX_RETRY_COUNT) {
        err_out_ << InputString << std::endl;
        err_out_ << e.what() << "\n";
      }
      // if Failed, Wait for 100 ms to try again
      int randRetryTime = rand() % 100 + 1;
      std::this_thread::sleep_for(std::chrono::milliseconds((100 + randRetryTime) * retryCount));
    }
  }
  return Status::Invalid("retry times exceeded max retry count");
}

void YSQLRelatedCustomerTxn::addCustomerSQL_(
    int w_id, std::vector<int> items, std::unordered_set<std::string>& customers,
    pqxx::work* txn) {
  std::string itemsStr = "";
  for (auto item : items) {
    itemsStr += std::to_string(item) + ",";
  }
  itemsStr.pop_back();
  LOG_INFO << "itemsStr: " << itemsStr;

  std::string query = format(
      "SELECT ol_w_id, ol_d_id, ol_o_id, count(*) "
      "FROM orderline "
      "WHERE ol_i_id IN (%s) "
      "AND ol_w_id != (%s) "
      "GROUP BY ol_w_id, ol_d_id, ol_o_id ",
      itemsStr.c_str(), std::to_string(w_id).c_str());
  pqxx::result result = txn->exec(query);
  if (result.empty()) {
    throw std::runtime_error("Warehouse not found");
  }
  for (auto row : result) {
    int ol_w_id = row["ol_w_id"].as<int>();
    int ol_d_id = row["ol_d_id"].as<int>();
    int ol_o_id = row["ol_o_id"].as<int>();
    int count = row["count"].as<int>();
    // LOG_INFO << "ol_w_id: " << ol_w_id << " ol_d_id: " << ol_d_id
    //          << " ol_o_id: " << ol_o_id << " count: " << count;
    if (count >= INCOMMON_THRESHOLD) {
      int c_id = getCustomerIdSQL_(ol_w_id, ol_d_id, ol_o_id, txn);
      std::string customer = "(" + std::to_string(ol_w_id) + ", " +
                             std::to_string(ol_d_id) + ", " +
                             std::to_string(c_id) + ")";
      if (customers.find(customer) == customers.end()) {
        customers.insert(customer);
      }
    }
  }
}

pqxx::result YSQLRelatedCustomerTxn::getOrdersSQL_(int c_w_id, int c_d_id,
                                                   int c_id, pqxx::work* txn) {
  std::string getOrdersSQL =
      "SELECT o_id FROM orders WHERE o_w_id = " + std::to_string(c_w_id) +
      " AND o_d_id = " + std::to_string(c_d_id) +
      " AND o_c_id = " + std::to_string(c_id) + ";";
  return txn->exec(getOrdersSQL);
}

pqxx::result YSQLRelatedCustomerTxn::getOrderLineSQL_(int o_w_id, int o_d_id,
                                                      int o_id,
                                                      pqxx::work* txn) {
  std::string getOrderLineSQL =
      "SELECT ol_i_id FROM orderline WHERE ol_w_id = " +
      std::to_string(o_w_id) + " AND ol_d_id = " + std::to_string(o_d_id) +
      " AND ol_o_id = " + std::to_string(o_id) + ";";
  return txn->exec(getOrderLineSQL);
}

int YSQLRelatedCustomerTxn::getCustomerIdSQL_(int w_id, int d_id, int o_id,
                                              pqxx::work* txn) {
  std::string getCustomerIdSQL = format(
      "SELECT o_c_id "
      "FROM orders "
      "WHERE o_w_id = %s "
      "AND o_d_id = %s "
      "AND o_id = %s",
      w_id, d_id, o_id);
  pqxx::result res = txn->exec(getCustomerIdSQL);
  if (res.empty()) {
    throw std::string("Customer not found");
  }
  return res[0]["o_c_id"].as<int>();
}
}  // namespace ydb_util