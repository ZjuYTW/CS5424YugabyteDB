#include "transaction.h"
#include <iostream>
#include "../util/bigint.cpp"
namespace transaction {
class Payment:public Transaction{
public:
    Payment(pqxx::connection *conn): Transaction(conn){};
    void process(std::string args[]) override{
        std::cout << "--------payment----------" << std::endl;
        int w_id = std::stoi(args[1]);
        int d_id = std::stoi(args[2]);
        int c_id = std::stoi(args[3]);
        double payment = std::stod(args[4]);
        //1. Update the warehouse C W ID by incrementing W YTD by PAYMENT
        bool update = true;
        do {
            pqxx::result wareHouse=getWarehouse(w_id);
            if (wareHouse.empty()){
                std::cout << "ware house not found" << std::endl;
                return;
            }
            double old_w_ytd = std::stod(wareHouse[0]["w_ytd"].c_str());
            double new_w_ytd = old_w_ytd + payment;
            bool update=updateWareHouse(w_id,old_w_ytd,new_w_ytd);
        } while (!update);

        //2. Update the district (C W ID,C D ID) by incrementing D YTD by PAYMENT
        do {
            pqxx::result wareHouse=getWarehouse(w_id);
            if (wareHouse.empty()){
                std::cout << "ware house not found" << std::endl;
                return;
            }
            double old_w_ytd = std::stod(wareHouse[0]["w_ytd"].c_str());
            double new_w_ytd = old_w_ytd + payment;
            bool update=updateWareHouse(w_id,old_w_ytd,new_w_ytd);
        } while (!update);


    }
private:
    bool updateWareHouse(int w_id,double old_w_ytd,double w_ytd) {
        try
        {
            pqxx::work txn(*conn);
            conn->prepare("updateWareHouse",
                "UPDATE warehouses "
                "SET w_ytd = $1"
                "WHERE w_id = $2 IF w_ytd = $3;"
            );
            pqxx::result contests = txn.exec_prepared("updateWareHouse",w_ytd,w_id,old_w_ytd);
            txn.commit();
            return true;
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
            return false;
        }
    }

    pqxx::result getDistrict(int w_id, int d_id){
        pqxx::work txn(*conn);
        pqxx::result res;
        std::cout << ">>>> Get District:" << std::endl;
        std::string query=format("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd FROM districts WHERE d_w_id = %d AND d_id = %d",w_id,d_id);
        std::cout<<query<<std::endl;
        res=txn.exec(query);
        for (auto row: res) {
            std::cout 
            << "d_street_1=" << row["d_street_1"].c_str() << ", "
            << "d_street_2=" << row["d_street_2"].c_str() << ", "
            << "d_city=" << row["d_city"].c_str() << ", "
            << "d_state=" << row["d_state"].c_str() 
            << "d_zip=" << row["d_zip"].c_str() << ", "
            << "d_ytd=" << row["d_ytd"].as<float>() 
            << std::endl;
        }
        txn.commit();
        return res;
    }

    pqxx::result getWarehouse(int w_id){
        pqxx::work txn(*conn);
        pqxx::result res;
        std::cout << ">>>> Get Warehouse:" << std::endl;
        std::string query=format("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd FROM warehouses WHERE w_id = %d",w_id);
        std::cout<<query<<std::endl;
        res=txn.exec(query);
        for (auto row: res) {
            std::cout 
            << "w_street_1=" << row["w_street_1"].c_str() << ", "
            << "w_street_2=" << row["w_street_2"].c_str() << ", "
            << "w_city=" << row["w_city"].c_str() << ", "
            << "w_state=" << row["w_state"].c_str() 
            << "w_zip=" << row["w_zip"].c_str() << ", "
            << "w_ytd=" << row["w_ytd"].as<float>() 
            << std::endl;
        }
        txn.commit();
        return res;
    }

    std::string format(const char *fmt, ...)
    {
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
}