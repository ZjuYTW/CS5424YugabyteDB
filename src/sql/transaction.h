#include <pqxx/pqxx>
namespace transaction {

class Transaction {
protected: 
    pqxx::connection *conn;
public:
    Transaction(pqxx::connection *conn) : conn(conn) {};
    virtual void process(std::string args[]);
};}