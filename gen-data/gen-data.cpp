#include <cstdlib>
#include <cmath>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <numeric>
#include <vector>
#include <random>

using namespace std;

int matIndex(int i, int j, int N)
{
    return N*j + i;
}

int main(int argc, char* argv[])
{
  double mean_increment = 0;
  double gen_min = 0.0;
  double gen_max = 10.0;
  int timeUnit = 100;
  double tolerance = 0.1;
  double increment_error = timeUnit*tolerance;

  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine generator (seed);

  if (argc < 3) {
    cerr << "USAGE: gen_rand numitems num_x num_y" << endl;
    return(EXIT_FAILURE);
  }

  normal_distribution<double> nd(mean_increment,increment_error);
  uniform_real_distribution<double> ud(gen_min, gen_max);
  istringstream numitems(argv[1]);
  istringstream numx(argv[2]);
  istringstream numy(argv[3]);

  int nIters, N, M;
  numitems >> nIters;
  numx >> N;
  numy >> M;

  // vector<double> nodes(N*M);

  nIters *= timeUnit;
  for (int iter=timeUnit; iter <= nIters; iter+=timeUnit) {
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < M; j++) {
        double increment = nd(generator);
        // nodes[matIndex(i,j,N)] += increment;
        std::cout << "{ \"x\" : " << i << ", \"y\": " << j << ", \"timestamp\" : " << iter+round(increment)
         << ", \"gen\": "<< ud(generator) << " }" << endl;
      }
    }
  }
  return 0;
}
