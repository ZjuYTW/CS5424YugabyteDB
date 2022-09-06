#ifndef YDB_PERF_PARSER_H_
#define YDB_PERF_PARSER_H_

#include <iostream>
#include <string>

namespace ydb_cql{
  class Parser{
    public:
    explicit Parser(std::string input){
      std::cout << input << std::endl; 
    } 

    void Foo() noexcept;
  };
}


#endif