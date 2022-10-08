#ifndef YDB_PERF_NEW_ORDER_TXN_H_
#define YDB_PERF_NEW_ORDER_TXN_H_
#include <vector>
#include <sys/time.h>
#include <ctime>

#include "common/txn/txn_type.h"

namespace ydb_util {

template <typename Connection>
class NewOrderTxn : public Txn<Connection> {
 public:
  explicit NewOrderTxn(Connection* conn)
      : Txn<Connection>(TxnType::new_order, conn) {}

  // NewOrder starts with N, C_ID, W_ID, D_ID, M
  // and follows M lines
  Status Init(const std::string& first_line, std::ifstream& ifs) noexcept {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 5) {
      return Status::AssertionFailed(
          "Expect NewOrderTxn has 5 first line args, but got " +
          std::to_string(ids.size()));
    }
    uint32_t m;
    // ignore first N
    // C_ID
    c_id_ = stoi(ids[1]);
    // W_ID
    w_id_ = stoi(ids[2]);
    // D_ID
    d_id_ = stoi(ids[3]);
    // M
    m = stoi(ids[4]);
    std::string tmp;
    orders_.reserve(m);
    for (int i = 0; i < m; i++) {
      getline(ifs, tmp);
      orders_.push_back(tmp);
    }
    return Status::OK();
  }

  Status ExecuteCQL() noexcept {
    CassError rc = CASS_OK;
    for (int i = 0; i < orders_.size(); i++) {
      uint32_t i_id, w_id, quantity;
      ParseOneOrder(orders_[i], &i_id, &w_id, &quantity);
      // Put your logic here, from Project New-Order Txn here
    }
    return Status::OK();
  }

  Status ExecuteSQL() noexcept { 
    std::vector<int> i_ids;
    std::vector<int> w_ids;
    std::vector<int> quantities;
    ParseArgs(i_ids, w_ids, quantities);

    int allLocal = 1;
    for (int i = 0; i < orders_.size(); i++) {
      if (w_ids[i] != w_id_) {
        allLocal = 0;
        break;
      }
    }

    int d_next_o_id = SQL_Get_D_Next_O_ID(w_id_, d_id_);
    SQL_Update_D_Next_O_ID(1, w_id_, d_id_);
    SQL_InsertNewOrder(d_next_o_id, allLocal);

    float total_amount = 0;
    for (int i = 0; i < orders_.size(); i++) {
      int s_quantity = SQL_Get_S_Quantity(w_ids[i], i_ids[i]);

      int adjust_qty = s_quantity - quantities[i];
      if (adjust_qty < 10) {
        adjust_qty += 100;
      }
      int remote_cnt = 0;
      if (w_ids[i] != w_id_) {
        remote_cnt = 1;
      }
      SQL_Update_S_Quantity(w_ids[i], i_ids[i], adjust_qty, quantities[i], 1, remote_cnt);

      float i_price = SQL_Get_I_Price(i_ids[i]);
      float item_amount = quantities[i] * i_price;
      total_amount += item_amount;

      char ol_amount[7];
      sprintf(ol_amount, "%.2f", item_amount);
      SQL_InsertNewOrderLine(d_next_o_id, i, i_ids[i], ol_amount, w_ids[i], quantities[i]);
    }

    float d_tax = SQL_Get_D_Tax(w_id_, d_id_);
    float w_tax = SQL_Get_W_Tax(w_id_);
    float c_discount = SQL_Get_C_Discount(w_id_, d_id_, c_id_);
    total_amount *= (1 + d_tax + w_tax) * (1 - c_discount);

    return Status::OK(); 
  }

 private:
  static Status ParseOneOrder(const std::string& order, uint32_t* i_id,
                              uint32_t* w_id, uint32_t* quantity) {
    auto ret = str_split(order, ',');
    if (ret.size() != 3) {
      return Status::AssertionFailed(
          "Expect one order line has 3 args, but got " +
          std::to_string(ret.size()));
    }
    *i_id = stoi(ret[0]);
    *w_id = stoi(ret[1]);
    *quantity = stoi(ret[2]);
    return Status::OK();
  }

  static Status ParseArgs(std::vector&<int> i_ids, std::vector&<int> w_ids,
                          std::vector&<int> quantities) {
    for (int i = 0; i < orders_.size(); i++) {
      auto ret = str_split(orders_[i], ',');
      if (ret.size() != 3) {
        return Status::AssertionFailed(
            "Expect one order line with 3 args, but got " +
            std::to_string(ret.size()));
      }
      i_ids.push_back(stoi(ret[0]));
      w_ids.push_back(stoi(ret[1]));
      quantities.push_back(stoi(ret[2]));
    }
    return Status::OK();
  }

  int SQL_Get_D_Next_O_ID(int w_id, int d_id) {
    pqxx::work txn(*conn);

    pqxx::result res = txn.exec("SELECT D_Next_O_ID FROM District WHERE D_W_ID = " + std::to_string(w_id) 
            + " AND D_ID = " + std::to_string(d_id));

    // std::cout 
    //       << "w_id=" << w_id<< ", "
    //       << "d_id=" << d_id << ", "
    //       << "D_Next_O_ID=" << n << std::endl;

    txn.commit();

    return res[0]["D_Next_O_ID"].as<int>;
  }

  void SQL_Update_D_Next_O_ID(int amount, int w_id, int d_id) {
      try {
          pqxx::work txn(*conn);

          txn.exec("UPDATE District SET D_Next_O_ID = D_Next_O_ID +" + std::to_string(amount) 
              + " WHERE D_W_ID = " + std::to_string(w_id) + " AND D_ID = " + std::to_string(d_id));

          txn.commit();
      } catch (pqxx::sql_error const &e) {
          if (e.sqlstate().compare("40001") == 0) {
              std::cerr << "The operation is aborted due to a concurrent transaction that is modifying the same set of rows." 
                        << "Consider adding retry logic or switch to the pessimistic locking." << std::endl;
          }
          throw e;   
      }
  }

  void SQL_InsertNewOrder(int n, int allLocal) {
      pqxx::work txn(*conn);

      char local_time_str[128];
      timeval current_time_tmp;
      gettimeofday(&current_time_tmp, NULL);
      char *local_time = NULL;
      local_time = get_local_time(local_time_str, sizeof(local_time_str), &current_time_tmp);

      txn.exec("INSERT INTO Order VALUES\
              (" + std::to_string(w_id_) + ", " + std::to_string(d_id_) + ", " + std::to_string(n) 
              + ", " + std::to_string(c_id_) + ", null, " + std::to_string(orders_.size()) + ","
              + ", " + std::to_string(allLocal) + ", " + local_time);
      
      std::cout 
          << "Order number=" << std::to_string(n) << ", "
          << "entry date=" << local_time << std::endl;

      txn.commit();

      // std::cout << ">>>> Successfully created table DemoAccount." << std::endl;
  }

  static char* get_local_time(char *time_str, int len, struct timeval *tv) {
      struct tm* ptm;                                   
      char time_string[40];                             
      long milliseconds;                                
                                                        
      ptm = localtime (&(tv->tv_sec));                  
                                                        
      // Output format: 2018-12-09 10:52:57.200         
      strftime(time_string, sizeof(time_string), "%Y-%m-%d %H:%M:%S", ptm);
      snprintf (time_str, len, "%s", time_string);
                                                        
      return time_str;                                  
  }

  int SQL_Get_S_Quantity(int w_id, int i_id) {
      pqxx::work txn(*conn);

      pqxx::result res = txn.exec("SELECT S_QUANTITY FROM Stock WHERE S_W_ID = " + std::to_string(w_id) 
              + " AND S_I_ID = " + std::to_string(i_id));

      txn.commit();

      return res[0]["S_QUANTITY"].as<int>;
  }

  void SQL_Update_S_Quantity(int w_id, int i_id, int s_quantity, int quantity, int order_cnt, int remote_cnt) {
      try {
          pqxx::work txn(*conn);

          txn.exec("UPDATE Stock SET S_QUANTITY = " + std::to_string(s_quantity) 
              + " WHERE S_W_ID = " + std::to_string(w_id) + " AND S_I_ID = " + std::to_string(i_id));

          txn.exec("UPDATE Stock SET S_YTD = S_YTD + " + std::to_string(quantity) 
              + " WHERE S_W_ID = " + std::to_string(w_id) + " AND S_I_ID = " + std::to_string(i_id));

          txn.exec("UPDATE Stock SET S_ORDER_CNT = S_ORDER_CNT + " + std::to_string(order_cnt) 
              + " WHERE S_W_ID = " + std::to_string(w_id) + " AND S_I_ID = " + std::to_string(i_id));
          
          txn.exec("UPDATE Stock SET S_REMOTE_CNT = S_REMOTE_CNT + " + std::to_string(remote_cnt) 
              + " WHERE S_W_ID = " + std::to_string(w_id) + " AND S_I_ID = " + std::to_string(i_id));

          txn.commit();
      } catch (pqxx::sql_error const &e) {
          if (e.sqlstate().compare("40001") == 0) {
              std::cerr << "The operation is aborted due to a concurrent transaction that is modifying the same set of rows." 
                        << "Consider adding retry logic or switch to the pessimistic locking." << std::endl;
          }
          throw e;   
      }
  }

  float SQL_Get_I_Price(int i_id) {
      pqxx::work txn(*conn);

      pqxx::result res = txn.exec("SELECT I_PRICE FROM Item WHERE I_ID = " + std::to_string(i_id)); 

      txn.commit();

      return res[0]["I_PRICE"].as<float>;
  }

  void SQL_InsertNewOrderLine(int n, int i, int item_number, char[7] ol_amount, int supply_w_id, int quantity) {
      pqxx::work txn(*conn);

      txn.exec("INSERT INTO Order-Line VALUES\
              (" + std::to_string(w_id_) + ", " + std::to_string(d_id_) + ", " + std::to_string(n) 
              + ", " + std::to_string(i) + "," + std::to_string(item_number) + ", null, " + ol_amount + ","
              + ", " + std::to_string(supply_w_id) + ", " + std::to_string(quantity) + ", S_DIST_" + std::to_string(d_id_));
      
      txn.commit();
  }

  float SQL_Get_D_Tax(int w_id, int d_id) {
      pqxx::work txn(*conn);

      pqxx::result res = txn.exec("SELECT D_TAX FROM District WHERE D_W_ID = " + std::to_string(w_id)
          + " AND D_ID = " + std::to_string(d_id)); 

      for (auto row: res) {
        std::cout 
            << "District tax rate=" << row["D_TAX"].as<float> << std::endl;
      }

      txn.commit();

      return res[0]["D_TAX"].as<float>;
  }

  float SQL_Get_W_Tax(int w_id) {
      pqxx::work txn(*conn);

      pqxx::result res = txn.exec("SELECT W_TAX FROM Warehouse WHERE W_ID = " + std::to_string(w_id));

      for (auto row: res) {
        std::cout 
            << "Warehouse tax rate=" << row["W_TAX"].as<float> << std::endl;
      }

      txn.commit();

      return res[0]["W_TAX"].as<float>;
  }

  float SQL_Get_C_Discount(int w_id, int d_id, int id) {
      pqxx::work txn(*conn);

      pqxx::result res = txn.exec("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM Customer WHERE C_W_ID = " + std::to_string(w_id)
          + " AND C_D_ID = " + std::to_string(d_id) + " AND C_ID = " + std::to_string(id));

      for (auto row: res) {
        std::cout 
            << "lastname=" << row["C_LAST"].c_str() << ", "
            << "credit=" << row["C_CREDIT"].c_str() << ", "
            << "discount=" << row["C_DISCOUNT"].as<float> << std::endl;
      }

      txn.commit();

      return res[0]["C_DISCOUNT"].as<float>;
  }

  std::vector<std::string> orders_;
  // Maybe change it into BigInt
  uint32_t c_id_, w_id_, d_id_;
};
};  // namespace ydb_util

#endif