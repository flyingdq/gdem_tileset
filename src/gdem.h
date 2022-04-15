#pragma once
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <queue>

#include "lrucache.hpp"
#include "rtree.hpp"
#include "state.h"

#define EARTH_RADIUS 6378137.0
#define EARTH_LENGTH 20037508.34

#ifndef M_PI
#define M_PI 3.14159265358979323846
/* 3.1415926535897932384626433832795 */
#endif

inline void mercatorToLonlat(double x, double y, double &lon, double &lat)
{
    lon = x / EARTH_LENGTH * 180.0;
    lat = y / EARTH_LENGTH * 180.0;
    lat = 180.0 / M_PI * (2 * atan(exp(lat * M_PI / 180.0)) - M_PI / 2.0);
}

inline void lonlatToMercator(double lon, double lat, double &x, double &y)
{
    x = lon / 180.0 * EARTH_LENGTH;
    y = log(tan((90.0 + lat) * M_PI / 360.0)) / (M_PI / 180.0);
    y = y * EARTH_LENGTH / 180.0;
}

#define NODATA -9999

typedef RTree<int, double, 2> DEMTree;

/**
 * @brief
 * block scope of lon/lat is (1.0 / 16.0 = 0.0625)
 * block width is (3600/16 + 1 = 226)
 */
struct DEMTileBlock
{
    DEMTileBlock(double west, double south)
    {
        this->west = west;
        this->south = south;
        this->data = nullptr;
    }

    ~DEMTileBlock()
    {
        if (this->data)
        {
            delete this->data;
            this->data = nullptr;
        }
    }

    double west;
    double south;
    int16_t *data;
};

struct TileCache
{
    TileCache(uint64_t size = 20480)
        : _size{size}
    {
    }

    void insert(int key, std::shared_ptr<DEMTileBlock> tile, State &state)
    {
        std::lock_guard<std::mutex> lock(mtx);
        auto iter = _map.find(key);
        if (iter != _map.end())
            return;

        _map[key] = tile;
        _queue.push(key);

        if (_queue.size() > _size)
        {
            int eraseKey = _queue.front();
            _queue.pop();
            _map.erase(eraseKey);
        }

        state.cacheSize = _queue.size();
    }

    bool tryGet(const int &key, std::shared_ptr<DEMTileBlock> &out)
    {
        std::lock_guard<std::mutex> lock(mtx);
        auto iter = _map.find(key);
        if (iter == _map.end())
            return false;

        out = iter->second;
        return true;
    }

    uint64_t _size;
    std::unordered_map<int, std::shared_ptr<DEMTileBlock>> _map;
    std::queue<int> _queue;
    std::mutex mtx;
};

class GdemPool
{
public:
    GdemPool();
    ~GdemPool();

    void init(std::vector<std::string> sources, int &max_lod, int tile_size, State &state);
    double getElevation(double lon, double lat, State &state);

    bool contains(double west, double south, double east, double north);
    void makeElevation(double west, double south, double east, double north, int width, int height, int16_t *data, State &state);
    void makeElevationImage(double west, double south, double east, double north,
                            int width, int height, std::string format, std::string type, std::string path, State &state);
    void makeElevationImage(int z, int x, int y, int width, int height,
                            std::string format, std::string type, std::string out_dir, State &state);

    void makeLodImage(int z, int x, int y, int width, int height,
                      std::string format, std::string type, std::string out_dir, State &state);

    void makeNullImage(int width, int height, std::string format, std::string type, std::string out_dir);

    // for debug
    void repairImage(int z, int x, int y, int width, int height,
                     std::string format, std::string type, std::string out_dir, State &state);

private:
    std::map<int, std::string> tile_map;
    TileCache tile_cache;
    DEMTree tile_tree;

    std::string default_projection;

    std::mutex repair_mutex;
};