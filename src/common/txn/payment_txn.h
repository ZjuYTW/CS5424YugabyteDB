#ifndef YDB_PERF_PAYMENT_TXN_H_
#define YDB_PERF_PAYMENT_TXN_H_
#include "common/txn/txn_type.h"

namespace ydb_util {
template <typename Connection>
class PaymentTxn : public Txn<Connection> {
 public:
  explicit PaymentTxn(Connection* conn)
      : Txn<Connection>(TxnType::payment, conn) {}

  Status ExecuteCQL() noexcept override{ return Status::OK(); }
  Status ExecuteSQL() noexcept override {
    // first update the warehouse
    if constexpr (std::is_same_v<Connection, pqxx::connection*>){
    auto conn = Txn<Connection>::GetConnection();
    pqxx::work txn(conn);
    int retryCount = 0;
    while (retryCount < MAX_RETRY_COUNT) {
      try
      {
        pqxx::row warehouse = getWarehouseSQL(w_id,txn);
        double old_w_ytd = std::stod(warehouse["w_ytd"].c_str());
        double new_w_ytd = old_w_ytd + payment;
        updateWareHouse(w_id,old_w_ytd,new_w_ytd,txn);
        pqxx::row district = getDistrictSQL(w_id,d_id,txn);
        double old_d_ytd = std::stod(district["d_ytd"].c_str());
        double new_d_ytd = old_w_ytd+ payment;
        



        txn.commit();
      }
      catch(const std::exception& e)
      {
        retryCount++;
        std::cerr << e.what() << '\n';
        // if Failed, Wait for 100 ms to try again
        std::this_thread::sleep_for(std::chrono::milliseconds(100 * retryCount));
      }
    }
      return Status::Invalid("retry times exceeded max retry count");
    }else{
      return Status::ConnectionFailed("the Connection is not SQL connections");
    }
  }
  Status Init(const std::string& first_line,
              std::ifstream& ifs) noexcept override {
    auto ids = str_split(first_line, ',');
    if (ids.size() != 5) {
      return Status::AssertionFailed(
          "Expect Payment has 5 first line args, but got " +
          std::to_string(ids.size()));
    }
    std::cout << "--------payment Init----------" << std::endl;
    w_id = std::stoi(ids[1]);
    d_id = std::stoi(ids[2]);
    c_id = std::stoi(ids[3]);
    payment = std::stod(ids[4]);
    return Status::OK();
  }

 private:
  uint32_t w_id, d_id, c_id;
  double payment;
  int MAX_RETRY_COUNT = 3;

  void updateWareHouseSQL(int w_id,double old_w_ytd,double w_ytd,pqxx::work txn) {
    if constexpr (std::is_same_v<Connection, pqxx::connection*>){
        pqxx::connection* conn = &txn.conn();
        conn->prepare("updateWareHouse",
            "UPDATE warehouses "
            "SET w_ytd = $1"
            "WHERE w_id = $2 IF w_ytd = $3;"
        );
        pqxx::result contests = txn.exec_prepared("updateWareHouse",w_ytd,w_id,old_w_ytd);
    }
    else{
      throw std::string("the Connection is not SQL connections");
    }
  }


  pqxx::row getDistrictSQL(int w_id, int d_id, pqxx::work txn) {
    if constexpr (std::is_same_v<Connection, pqxx::connection*>){
      pqxx::result res;
      std::cout << ">>>> Get District:" << std::endl;
      std::string query = format(
        "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd FROM "
        "districts WHERE d_w_id = %d AND d_id = %d",
        w_id, d_id);
      std::cout << query << std::endl;
    res = txn.exec(query);
    if (res.empty()) {
      throw std::string("District not found");
    }
    for (auto row : res) {
      std::cout << "d_street_1=" << row["d_street_1"].c_str() << ", "
                << "d_street_2=" << row["d_street_2"].c_str() << ", "
                << "d_city=" << row["d_city"].c_str() << ", "
                << "d_state=" << row["d_state"].c_str()
                << "d_zip=" << row["d_zip"].c_str() << ", "
                << "d_ytd=" << row["d_ytd"].as<float>() << std::endl;
    }
    return res[0];
    }else{
      throw std::string("the Connection is not SQL connections");
    }
  }
  pqxx::row getWarehouseSQL(int w_id, pqxx::work txn) {
    
    if constexpr (std::is_same_v<Connection, pqxx::connection*>){
      pqxx::result res;
          std::cout << ">>>> Get Warehouse:" << std::endl;
    std::string query = format(
        "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd FROM "
        "warehouses WHERE w_id = %d",
        w_id);
    std::cout << query << std::endl;
    res = txn.exec(query);
    if (res.empty()) {
      throw std::string("Warehouse not found");
    }
    for (auto row : res) {
      std::cout << "w_street_1=" << row["w_street_1"].c_str() << ", "
                << "w_street_2=" << row["w_street_2"].c_str() << ", "
                << "w_city=" << row["w_city"].c_str() << ", "
                << "w_state=" << row["w_state"].c_str()
                << "w_zip=" << row["w_zip"].c_str() << ", "
                << "w_ytd=" << row["w_ytd"].as<float>() << std::endl;
    }
    return res[0];
    }else{
      throw std::string("the Connection is not SQL connections");
    }
  }

  std::string format(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    const auto len = vsnprintf(nullptr, 0, fmt, args);
    va_end(args);
    std::string r;
    r.resize(static_cast<size_t>(len) + 1);
    va_start(args, fmt);
    vsnprintf(&r.front(), len + 1, fmt, args);
    va_end(args);
    r.resize(static_cast<size_t>(len));
    return r;
  }
};

}  // namespace ydb_util
#endif