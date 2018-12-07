#pragma once

#include <iostream>

namespace nebula {
namespace test {

class TestClass {
private:
  int value_;

public:
  TestClass(int value) : value_{value} {}
  virtual ~TestClass() { std::cout << "value=" << value_ << std::endl; };
};

} // namespace test
} // namespace nebula