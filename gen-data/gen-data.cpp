#include <cstdlib>
#include <cmath>
#include <chrono>
#include <iostream>
#include <numeric>
#include <vector>
#include <random>

using namespace std;

int main()
{
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator (seed);

  constexpr int TOTAL = 100000000;
  normal_distribution<double> nd;
  vector<double> v;
  v.reserve(TOTAL);
  for (int i=0; i< TOTAL; i++) v.push_back(nd(generator));

  double avg = accumulate(v.begin(),v.end(), 0.0, [](double x, double y) { return x+y;});
  cout << avg /TOTAL << endl;
  return 0;
}
