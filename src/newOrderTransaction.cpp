#include <iostream>
#include <pqxx/pqxx>

int get_D_Next_O_ID(int w_id, int d_id);

void newOrderTransaction(pqxx::connection *conn, int w_id, int d_id, int c_id, int num_items, int[] item_number, int[] supplier_warehouse, int[] quantity) {
    try {
        pqxx::work txn(*conn);

        int n = get_D_Next_O_ID(w_id, d_id);
        int local = 

        txn.exec("INSERT INTO Order VALUES\
                    (" + std::to_string(n) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id) 
                    + ", " + std::to_string(c_id) + ", " + ", " + std::to_string(num_items) + ","
                    + ", " + std::to_string(local) + ", " + std::to_string(num_items);

        txn.exec(format("INSERT INTO Order VALUES \
                (%d, %d, 28, 'USA', 10000)");

        txn.commit();

        std::cout << ">>>> Transferred " << amount << " between accounts." << std::endl;
    } catch (pqxx::sql_error const &e) {
        if (e.sqlstate().compare("40001") == 0) {
            std::cerr << "The operation is aborted due to a concurrent transaction that is modifying the same set of rows." 
                      << "Consider adding retry logic or switch to the pessimistic locking." << std::endl;
        }
        throw e; 
    }
}

int get_D_Next_O_ID(int w_id, int d_id) {
    pqxx::work txn(*conn);

    int n = txn.exec("SELECT D_Next_O_ID FROM District WHERE D_W_ID = " + std::to_string(w_id) 
            + " AND D_ID = " + std::to_string(d_id));

    std::cout 
          << "w_id=" << w_id<< ", "
          << "d_id=" << d_id << ", "
          << "D_Next_O_ID=" << n << std::endl;

    txn.commit();

    return n;
}

int get_D_Next_O_ID(int w_id, int d_id) {
    pqxx::work txn(*conn);

    int n = txn.exec("SELECT D_Next_O_ID FROM District WHERE D_W_ID = " + std::to_string(w_id) 
            + " AND D_ID = " + std::to_string(d_id));

    std::cout 
          << "w_id=" << w_id<< ", "
          << "d_id=" << d_id << ", "
          << "D_Next_O_ID=" << n << std::endl;

    txn.commit();

    return n;
}
