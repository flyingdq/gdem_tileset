#pragma once
#include <string>
#include <map>
#include <mutex>
#include <atomic>

using namespace std;

struct State
{
    string name = "";
    atomic_int64_t tilesTotal = 0;
    atomic_int64_t tilesProcessed = 0;
    double duration = 0.0;
    std::map<string, string> values;

    int64_t cacheSize = 0;

    int numPasses = 0;
    int currentPass = 0; // starts with index 1! interval: [1,  numPasses]

    mutex mtx;

    double progress()
    {
        return double(tilesProcessed) / double(tilesTotal);
    }
};