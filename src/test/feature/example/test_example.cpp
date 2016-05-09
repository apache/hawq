#include <vector>
#include "lib/common.h"

int main(int argc, char *argv[])
{
  std::vector<int> A = {1, 2, 3};
  std::vector<int> B = {1, 2, 3};
  hawq::test::EQUAL_CHECK(A, B);
  return 0;
}
