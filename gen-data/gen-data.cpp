#include <cstdlib>
#include <cmath>
#include <chrono>
#include <iostream>
#include <sstream>
#include <numeric>
#include <vector>
#include <random>

using namespace std;

int hash(int i, int j, int N)
{
    return N*j + i;
}

int main(int argc, char* argv[])
{
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator (seed);
  if (argc < 3) {
    cerr << "USAGE: gen_rand numitems num_x num_y" << endl;
    return(EXIT_FAILURE);
  }

  normal_distribution<double> nd;

  istringstream numitems(argv[1]);
  istringstream numx(argv[2]);
  istringstream numy(argv[3]);

  int nIters, N, M;
  numitems >> nIters;
  numx >> N;
  numy >> M;

  vector<double>(N*M);

  for (iter=0; iter < nIters; iter++) {

  }
  // USAGE: gen_rand numitems num_x num_y
  for (int i=0; i< nIters; i++) v.push_back(nd(generator));

  double avg = accumulate(v.begin(),v.end(), 0.0, [](double x, double y) { return x+y;});
  cout << avg /nIters << endl;
  return 0;
}
