#define NOMINMAX

#include "arguments/Arguments.hpp"
#include "unsuck.hpp"
#include "threadpool.hpp"
#include "TaskPool.hpp"
#include "logger.h"
#include "gdem.h"
#include "state.h"

#include <iostream>
using namespace std;

struct Monitor
{
    thread t;
    bool stopRequested = false;

    void stop()
    {

        stopRequested = true;

        t.join();
    }
};

shared_ptr<Monitor> startMonitoring(State &state)
{
    shared_ptr<Monitor> monitor = make_shared<Monitor>();

    monitor->t = thread([monitor, &state]()
                        {
                            using namespace std::chrono_literals;

                            std::this_thread::sleep_for(1'000ms);

                            while (!monitor->stopRequested)
                            {
                                // if (state.duration < 0.000001)
                                //     continue;

                                auto ram = getMemoryData();
                                auto CPU = getCpuData();
                                double GB = 1024.0 * 1024.0 * 1024.0;

                                double throughput = (double(state.tilesProcessed) / state.duration) / 1'000'000.0;
                                int64_t tilesProcessed = state.tilesProcessed;
                                string strTilesProcessed = formatNumber(tilesProcessed);

                                double progressPass = 100.0 * state.progress();
                                double progressTotal = (100.0 * double(state.currentPass - 1) + progressPass) / double(state.numPasses);

                                string strProgressPass = formatNumber(progressPass) + "%";
                                string strProgressTotal = formatNumber(progressTotal) + "%";
                                string strTime = formatNumber(now()) + "s";
                                string strDuration = formatNumber(state.duration) + "s";
                                string strThroughput = formatNumber(throughput) + "MPs";

                                string strRAM = formatNumber(double(ram.virtual_usedByProcess) / GB, 1) + "GB (highest " + formatNumber(double(ram.virtual_usedByProcess_max) / GB, 1) + "GB)";
                                string strCPU = formatNumber(CPU.usage) + "%";

                                string cacheSize = formatNumber(state.cacheSize);

                                stringstream ss;
                                ss << "[" << strProgressTotal << ", " << strTime << "], "
                                   << "[" << state.name << ": " << strProgressPass << ", duration: " << strDuration << ", tilesProcessed: " << strTilesProcessed << "]"
                                   << "[RAM: " << strRAM << ", CPU: " << strCPU << ", CacheSize: " << cacheSize << "]";

                                cout << ss.str() << endl;

                                std::this_thread::sleep_for(1'000ms);
                            } });

    return monitor;
}

void tileset(GdemPool &gdem_pool, State &state, int max_lod, int tile_size, string out_format, string out_type, string outdir)
{
    cout << endl;
    cout << "=======================================" << endl;
    cout << "=== tileset                           " << endl;
    cout << "=======================================" << endl;

    auto tStart = now();

    int64_t ztilesTotal = 2;
    for (int z = 1; z <= max_lod; z++)
    {
        ztilesTotal = ztilesTotal * 4;
    }

    state.name = "tileset";
    state.currentPass = 2;
    state.tilesTotal = ztilesTotal;
    state.tilesProcessed = 0;
    state.duration = 0;

    struct Task
    {
        int z;
        int x;
        int y;

        Task(int z, int x, int y)
        {
            this->z = z;
            this->x = x;
            this->y = y;
        }
    };

    atomic_uint32_t active_tasks = 0;
    size_t numThreads = getCpuData().numProcessors * 2;
    int64_t tilesProcessed = 0;
    double lastReport = now();
    mutex mtx;
    TaskPool<Task> pool(
        numThreads, [&](auto task)
        {
            gdem_pool.makeElevationImage(task->z, task->x, task->y, tile_size, tile_size, out_format, out_type, outdir,state);
            active_tasks--;

            lock_guard<mutex> lock(mtx);

            tilesProcessed = tilesProcessed + 1;
            if (now() - lastReport > 1.0)
            {
                state.tilesProcessed = tilesProcessed;
                state.duration = now() - tStart;

                lastReport = now();
            } 
        });

    int z = max_lod;
    {
        fs::create_directories(outdir + "/" + formatNumber(z));
        int x_num = 2 << z;
        int y_num = 1 << z;
        for (int x = 0; x < x_num; x++)
        {
            double x_step = 360.0 / x_num;
            if(!gdem_pool.contains(-180.0 + x * x_step, -90.0, -180.0 + x * x_step + x_step, 90.0))
            {
                lock_guard<mutex> lock(mtx);

                tilesProcessed = tilesProcessed + y_num;
                if (now() - lastReport > 1.0)
                {
                    state.tilesProcessed = tilesProcessed;
                    state.duration = now() - tStart;

                    lastReport = now();
                } 
                continue;
            }

            fs::create_directories(outdir + "/" + formatNumber(z) + "/" + formatNumber(x));
            for (int y = 0; y < y_num; y++)
            {
                while (true)
                {
                    if (active_tasks > 10000)
                    {
                        std::this_thread::sleep_for(10ms);
                    }
                    else
                    {
                        break;
                    }
                }
                auto task = make_shared<Task>(z, x, y);
                pool.addTask(task);
                active_tasks++;
            }
        }
    }

    pool.waitTillEmpty();
    pool.close();

    double duration = now() - tStart;
    state.values["duration(tileset)"] = formatNumber(duration, 3);
}

void makelod(GdemPool &gdem_pool, State &state, int max_lod, int tile_size, string out_format, string out_type, string outdir)
{
    cout << endl;
    cout << "=======================================" << endl;
    cout << "=== makelod                           " << endl;
    cout << "=======================================" << endl;

    auto tStart = now();

    int64_t tilesTotal = 2;
    int64_t ztilesTotal = 2;
    for (int z = 1; z <= max_lod - 1; z++)
    {
        ztilesTotal = ztilesTotal * 4;
        tilesTotal += ztilesTotal;
    }

    state.name = "makelod";
    state.currentPass = 3;
    state.tilesTotal = tilesTotal;
    state.tilesProcessed = 0;
    state.duration = 0;

    struct Task
    {
        int z;
        int x;
        int y;

        Task(int z, int x, int y)
        {
            this->z = z;
            this->x = x;
            this->y = y;
        }
    };

    atomic_uint32_t active_tasks = 0;
    size_t numThreads = getCpuData().numProcessors * 2;
    int64_t tilesProcessed = 0;
    double lastReport = now();
    mutex mtx;
    TaskPool<Task> pool(
        numThreads, [&](auto task)
        {
            gdem_pool.makeLodImage(task->z, task->x, task->y, tile_size, tile_size, out_format, out_type, outdir,state);
            active_tasks--;

            lock_guard<mutex> lock(mtx);

            tilesProcessed = tilesProcessed + 1;
            if (now() - lastReport > 1.0)
            {
                state.tilesProcessed = tilesProcessed;
                state.duration = now() - tStart;

                lastReport = now();
            } });

    for (int z = max_lod - 1; z >= 0; z--)
    {
        // make sure all sub tiles are ready
        pool.waitTillEmpty();
        std::this_thread::sleep_for(2s);

        fs::create_directories(outdir + "/" + formatNumber(z));
        int x_num = 2 << z;
        int y_num = 1 << z;
        for (int x = 0; x < x_num; x++)
        {
            double x_step = 360.0 / x_num;
            if(!gdem_pool.contains(-180.0 + x * x_step, -90.0, -180.0 + x * x_step + x_step, 90.0))
            {
                lock_guard<mutex> lock(mtx);

                tilesProcessed = tilesProcessed + y_num;
                if (now() - lastReport > 1.0)
                {
                    state.tilesProcessed = tilesProcessed;
                    state.duration = now() - tStart;

                    lastReport = now();
                } 
                continue;
            }

            fs::create_directories(outdir + "/" + formatNumber(z) + "/" + formatNumber(x));
            for (int y = 0; y < y_num; y++)
            {
                while (true)
                {
                    if (active_tasks > 100)
                    {
                        std::this_thread::sleep_for(10ms);
                    }
                    else
                    {
                        break;
                    }
                }
                auto task = make_shared<Task>(z, x, y);
                pool.addTask(task);
                active_tasks++;
            }
        }
    }

    pool.waitTillEmpty();
    pool.close();

    double duration = now() - tStart;
    state.values["duration(makelod)"] = formatNumber(duration, 3);
}

int main(int argc, char **argv)
{
    double tStart = now();

    auto exePath = fs::canonical(fs::absolute(argv[0])).parent_path().string();

    launchMemoryChecker(4 * 1024, 0.1);
    auto cpuData = getCpuData();

    cout << endl
         << "Version 1.0 by FLING(GDET)" << endl
         << endl;

    Arguments args(argc, argv);
    args.addArgument("help,h", "Display help information");
    args.addArgument("source,i,", "Input file(s) or dir(s) of the gdem");
    args.addArgument("outdir,o", "output directory");
    args.addArgument("no_log", "not to write log info");
    args.addArgument("max_lod", "max_lod of tileset, -1 default, -1 means use the calulated max lod by gdem size and tile_size");
    args.addArgument("tile_size", "tile pixel size, 256 default");
    args.addArgument("out_format", "output image format, grey default, [grey, rgba]");
    args.addArgument("out_type", "output image type, png default, [png, tif]");
    args.addArgument("mercator", "out tileset is mercator projection, nums of x is 1 at level 0, nums of y is 1 at level 0");
    args.addArgument("no_tileset", "skip tileset process");

    if (args.has("help"))
    {
        cout << endl
             << args.usage() << endl;
        exit(0);
    }

    if (!args.has("source"))
    {
        cout << "gdem_tileset <source> -o <outdir>" << endl;
        cout << endl
             << "For a list of options, use --help or -h" << endl;

        exit(1);
    }

    vector<string> source = args.get("source").as<vector<string>>();
    if (source.size() == 0)
    {
        cout << "gdem_tileset <source> -o <outdir>" << endl;
        cout << endl
             << "For a list of options, use --help or -h" << endl;

        exit(1);
    }

    string outdir = "";
    if (args.has("outdir"))
    {
        outdir = args.get("outdir").as<string>();
    }
    else
    {
        string sourcepath = source[0];
        fs::path path(sourcepath);

        if (!fs::exists(path))
        {
            exit(123);
        }

        path = fs::canonical(path);

        string suggestedBaseName = path.filename().string() + "_tileset";
        outdir = sourcepath + "/../" + suggestedBaseName;
    }
    outdir = fs::weakly_canonical(fs::path(outdir)).string();
    fs::create_directories(outdir);

    if (!args.has("no_log"))
    {
        logger::addOutputFile(outdir + "/log.txt");
    }

    int max_lod = args.get("max_lod").as<int>(-1);
    int tile_size = args.get("tile_size").as<int>(256);
    string out_format = args.get("out_format").as<string>("grey");
    string out_type = args.get("out_type").as<string>("png");
    bool has_tileset = !args.has("no_tileset");

    State state;
    state.numPasses = 3;
    auto monitor = startMonitoring(state);

    GdemPool gdem_pool;
    gdem_pool.init(source, max_lod, tile_size, state);

    // gdem_pool.repairImage(11, 837, 416, tile_size, tile_size, out_format, out_type, outdir, state);
    // return 0;

    //double ele = gdem_pool.getElevation(120.81127,23.24386, state);
    //ele = gdem_pool.getElevation(120.81545,23.24706, state);

    // string path = outdir + "/" + formatNumber(0) + "/" + formatNumber(0);
    // fs::create_directories(path);
    // gdem_pool.makeElevationImage(0, 0, 0, tile_size, tile_size, out_format, out_type, outdir);

    // gdem_pool.makeElevationImage(12, 1674, 820, tile_size, tile_size, out_format, out_type, outdir, state);
    //return 0;

    // gdem_pool.makeNullImage(256, 256, "rgba", "png", "H:\\");
    // return 0;

    if (has_tileset)
        tileset(gdem_pool, state, max_lod, tile_size, out_format, out_type, outdir);

    makelod(gdem_pool, state, max_lod, tile_size, out_format, out_type, outdir);

    gdem_pool.makeNullImage(tile_size, tile_size, out_format, out_type, outdir);

    monitor->stop();

    double duration = now() - tStart;

    cout << endl;
    cout << "=======================================" << endl;
    cout << "=== STATS                              " << endl;
    cout << "=======================================" << endl;

    cout << "output location:       " << outdir << endl;

    for (auto [key, value] : state.values)
    {
        cout << key << ": \t" << value << endl;
    }

    cout << "duration:              " << formatNumber(duration, 3) << "s" << endl;

    return 0;
}