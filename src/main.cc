#include <iostream>
#include "parser/parser.h"

int main(int argc, char *argv[]) {
  std::cout << "Hello YDB project!" << std::endl;
  ydb_cql::Parser p("test");
  p.Foo();
  return 0; 
}