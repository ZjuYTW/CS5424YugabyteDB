#include "ysql_impl//sql_txn/new_order_txn.h"

#include <sys/time.h>

#include <pqxx/pqxx>

#include "common/util/string_util.h"
#include "thread"

namespace ydb_util {
float YSQLNewOrderTxn::Execute() noexcept {
  LOG_INFO << "New Order Transaction started";

  time_t start_t, end_t;
  double diff_t;
  time(&start_t);
  int retryCount = 0;
  auto NewOrder= format("N %d %d %d",w_id_,d_id_,c_id_);
  while (retryCount < MAX_RETRY_COUNT) {
    try {
      int allLocal = 1;
      for (int i = 0; i < orders_.size(); i++) {
        if (w_ids[i] != w_id_) {
          allLocal = 0;
          break;
        }
      }

      pqxx::work txn(*conn_);
      int d_next_o_id = SQL_Get_D_Next_O_ID(w_id_, d_id_, &txn);
      SQL_Update_D_Next_O_ID(1, w_id_, d_id_, &txn);
      SQL_InsertNewOrder(d_next_o_id, allLocal, &txn);

      float total_amount = 0;
      for (int i = 0; i < orders_.size(); i++) {
        int s_quantity = SQL_Get_S_Quantity(w_ids[i], i_ids[i], &txn);

        int adjust_qty = s_quantity - quantities[i];
        if (adjust_qty < 10) {
          adjust_qty += 100;
        }
        int remote_cnt = 0;
        if (w_ids[i] != w_id_) {
          remote_cnt = 1;
        }
        SQL_Update_S_Quantity(w_ids[i], i_ids[i], adjust_qty, quantities[i], 1,
                              remote_cnt, &txn);

        float i_price = SQL_Get_I_Price(i_ids[i], &txn);
        float item_amount = quantities[i] * i_price;
        total_amount += item_amount;

        outputs[outputs.size() - 1] +=
            (format(", SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %d, "
                    "S_QUANTITY: %d",
                    w_ids[i], quantities[i], item_amount, s_quantity));
        outputs.push_back("");
        //        std::cout
        //            << "Supplier Warehouse=" << w_ids[i] << ", "
        //            << "Quantity=" << quantities[i] << ", "
        //            << "OL_AMOUNT=" << item_amount << ", "
        //            << "S_QUANTITY=" << s_quantity << std::endl;

        SQL_InsertNewOrderLine(d_next_o_id, i + 1, i_ids[i], item_amount,
                               w_ids[i], quantities[i], &txn);
      }

      float d_tax = SQL_Get_D_Tax(w_id_, d_id_, &txn);
      float w_tax = SQL_Get_W_Tax(w_id_, &txn);
      float c_discount = SQL_Get_C_Discount(w_id_, d_id_, c_id_, &txn);
      total_amount *= (1 + d_tax + w_tax) * (1 - c_discount);
      txn.commit();
      outputs.push_back(format("NUM_ITEMS: %d, TOTAL_AMOUNT: %d",
                               orders_.size(), total_amount));
      //      std::cout
      //          << "Number of items=" << orders_.size() << ", "
      //          << "Total amount=" << total_amount << std::endl;

      for (auto & output : outputs) {
        txn_out_ << output+"\n";
      }

      time(&end_t);
      diff_t = difftime(end_t, start_t);
      return diff_t;
    } catch (const std::exception& e) {
      retryCount++;
      LOG_ERROR << e.what();
      if (retryCount==MAX_RETRY_COUNT){
        err_out_<<e.what()<<"\n";
      }
      // if Failed, Wait for 100 ms to try again
      // TODO: check if there is a sleep_for
      std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
    }
  }
  return 0;
}

int YSQLNewOrderTxn::SQL_Get_D_Next_O_ID(int w_id, int d_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get D_Next_O_ID:";
  std::string query =
      format("SELECT D_Next_O_ID FROM District WHERE D_W_ID = %d AND D_ID = %d",
             w_id, d_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("D_Next_O_ID not found");
  }

  for (auto row : res) {
    LOG_INFO << "D_W_ID=" << w_id << ", "
             << "D_ID=" << d_id << ", "
             << "D_Next_O_ID=" << row["D_Next_O_ID"].as<int>();
  }
  return res[0]["D_Next_O_ID"].as<int>();
}

void YSQLNewOrderTxn::SQL_Update_D_Next_O_ID(int amount, int w_id, int d_id,
                                             pqxx::work* txn) {
  LOG_INFO << ">>>> Update D_Next_O_ID:";
  std::string query = format(
      "UPDATE District SET D_Next_O_ID = D_Next_O_ID + %d WHERE D_W_ID = %d "
      "AND D_ID = %d",
      amount, w_id, d_id);
  LOG_INFO << query;
  txn->exec(query);
}

void YSQLNewOrderTxn::SQL_InsertNewOrder(int n, int allLocal, pqxx::work* txn) {
  LOG_INFO << ">>>> Insert New Order:";
  char local_time_str[128];
  timeval current_time_tmp;
  gettimeofday(&current_time_tmp, NULL);
  char* local_time = NULL;
  local_time =
      get_local_time(local_time_str, sizeof(local_time_str), &current_time_tmp);
  std::string query = format(
      "INSERT INTO orders VALUES"
      "(%d, %d, %d, %d, null, %d, %d, '%s')",
      w_id_, d_id_, n, c_id_, orders_.size(), allLocal, local_time);
  LOG_INFO << query;
  txn->exec(query);

  outputs.push_back(format("O_ID: %d, O_ENTRY_D: %s", n, local_time));
  outputs.push_back("");
  //  std::cout
  //      << "Order number=" << std::to_string(n) << ", "
  //      << "entry date=" << local_time << std::endl;
}

char* YSQLNewOrderTxn::get_local_time(char* time_str, int len,
                                      struct timeval* tv) {
  struct tm* ptm;
  char time_string[40];
  long milliseconds;

  ptm = localtime(&(tv->tv_sec));

  // Output format: 2018-12-09 10:52:57.200
  strftime(time_string, sizeof(time_string), "%Y-%m-%d %H:%M:%S", ptm);
  milliseconds = tv->tv_usec / 10;

  snprintf(time_str, len, "%s.%05ld", time_string, milliseconds);
  return time_str;
}

int YSQLNewOrderTxn::SQL_Get_S_Quantity(int w_id, int i_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get S_Quantity:";
  std::string query =
      format("SELECT S_QUANTITY FROM Stock WHERE S_W_ID = %d AND S_I_ID = %d",
             w_id, i_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("S_Quantity not found");
  }

  //  for (auto row : res) {
  //    LOG_INFO << "D_W_ID=" << row["D_W_ID"].as<int>() << ", "
  //             << "D_ID=" << row["D_ID"].as<int>() << ", "
  //             << "D_Next_O_ID=" << row["D_Next_O_ID"].as<int>();
  //  }
  return res[0]["S_QUANTITY"].as<int>();
}

void YSQLNewOrderTxn::SQL_Update_S_Quantity(int w_id, int i_id, int s_quantity,
                                            int quantity, int order_cnt,
                                            int remote_cnt, pqxx::work* txn) {
  std::string query = format(
      "UPDATE Stock SET S_QUANTITY = %d, S_YTD = S_YTD + %d, S_ORDER_CNT = "
      "S_ORDER_CNT + %d, S_REMOTE_CNT = S_REMOTE_CNT + %d WHERE S_W_ID = %d "
      "AND S_I_ID = %d",
      s_quantity, quantity, order_cnt, remote_cnt, w_id, i_id);
  LOG_INFO << query;
  txn->exec(query);
}

float YSQLNewOrderTxn::SQL_Get_I_Price(int i_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get I_Price:";
  std::string query =
      format("SELECT I_PRICE, I_NAME FROM Item WHERE I_ID = %d", i_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("I_Price not found");
  }

  for (auto row : res) {
    outputs.push_back(
        format("ITEM_NUMBER: %d, I_NAME: %d", i_id, row["I_NAME"].c_str()));
    //    std::cout
    //        << "I_NAME=" << row["I_NAME"].c_str() << ", "
    //        << "Item Number=" << i_id << std::endl;
  }

  return res[0]["I_Price"].as<float>();
}

void YSQLNewOrderTxn::SQL_InsertNewOrderLine(int n, int i, int item_number,
                                             float ol_amount, int supply_w_id,
                                             int quantity, pqxx::work* txn) {
  LOG_INFO << ">>>> Insert New OrderLine:";
  std::string query = format(
      "INSERT INTO orderline VALUES"
      "(%d, %d, %d, %d, %d, null, %.2f, %d, %d, 'S_DIST_%d')",
      w_id_, d_id_, n, i, item_number, ol_amount, supply_w_id, quantity, d_id_);
  LOG_INFO << query;
  txn->exec(query);
}

float YSQLNewOrderTxn::SQL_Get_D_Tax(int w_id, int d_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get D_Tax:";
  std::string query = format(
      "SELECT D_TAX FROM District WHERE D_W_ID = %d AND D_ID = %d", w_id, d_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("D_Tax not found");
  }

  for (auto row : res) {
    outputs.push_back(format("D_TAX: %f", row["D_TAX"].as<float>()));
    outputs.push_back("");
    //    std::cout
    //        << "District tax rate=" << row["D_TAX"].as<float>() << std::endl;
  }

  return res[0]["D_Tax"].as<float>();
}

float YSQLNewOrderTxn::SQL_Get_W_Tax(int w_id, pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get W_Tax:";
  std::string query =
      format("SELECT W_TAX FROM Warehouse WHERE W_ID = %d", w_id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("W_Tax not found");
  }

  for (auto row : res) {
    outputs.push_back(format("W_TAX: %f", row["W_TAX"].as<float>()));
    outputs.push_back("");
    //    std::cout
    //        << "Warehouse tax rate=" << row["W_TAX"].as<float>() << std::endl;
  }
  return res[0]["W_TAX"].as<float>();
}

float YSQLNewOrderTxn::SQL_Get_C_Discount(int w_id, int d_id, int id,
                                          pqxx::work* txn) {
  pqxx::result res;
  LOG_INFO << ">>>> Get C_Discount:";
  std::string query = format(
      "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM Customer WHERE C_W_ID = %d AND "
      "C_D_ID = %d AND C_ID = %d",
      w_id, d_id, id);
  LOG_INFO << query;
  res = txn->exec(query);

  if (res.empty()) {
    throw std::runtime_error("C_Discount not found");
  }

  for (auto row : res) {
    outputs.push_back(
        format("Customer identifier (W_ID: %d, D_ID: %d, C_ID: %d)",
               "C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %f", w_id, d_id, id,
               row["C_LAST"].c_str(), row["C_CREDIT"].c_str(),
               row["C_DISCOUNT"].as<float>()));
    outputs.push_back("");
    //    std::cout
    //        << "Customer lastname=" << row["C_LAST"].c_str() << ", "
    //        << "credit=" << row["C_CREDIT"].c_str() << ", "
    //        << "discount=" << row["C_DISCOUNT"].as<float>() << std::endl;
  }

  return res[0]["C_DISCOUNT"].as<float>();
}

}  // namespace ydb_util