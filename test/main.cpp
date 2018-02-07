// +build ignore

#include "tdigest.h"
#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <vector>
#include <iomanip>

using namespace tdigest;

double quantiles[7] = {
    0.1,
    0.2,
    0.5,
    0.75,
    0.9,
    0.99,
    0.999,
};


std::vector<double> loadData(std::string name) {
    std::ifstream f (name);
    std::vector<double> data;

    f >> std::setprecision(std::numeric_limits<long double>::digits10 + 1);
    double x;
    while (f >> x) {
        data.push_back(x);
    }
    return data;
}

std::vector<double> computeQuantiles(std::vector<double> data){
    TDigest* td = new TDigest(1000);

    for (auto x : data) {
        td->add(x);
    }
    std::vector<double> results;

    for (int i = 0; i < 7; i++) {
        double q = td->quantile(quantiles[i]);
        results.push_back(q);
    }

    return results;
}

void writeResults(std::string name, std::vector<double> results){
    std::ofstream f (name);

    f << std::setprecision(std::numeric_limits<long double>::digits10 + 1);
    for (int i = 0; i < 7; i++) {
        f << results[i] << " " << quantiles[i] << std::endl;
    }
}

int main() {
    std::string dataFiles[3] = {"small.dat", "uniform.dat", "normal.dat"};
    for (int i = 0; i < 3; i++) {
        std::vector<double> data = loadData(dataFiles[i]);
        auto results = computeQuantiles(data);
        writeResults(dataFiles[i] + ".cpp.quantiles", results);
    }
    return 0;
}
