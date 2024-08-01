/**
 * @file gdem.cpp
 * @author fling
 * @brief
 * @version 0.1
 * @date 2022-03-09
 *
 * @copyright Copyright (c) 2022
 *
 *
 * GDEM数据一块的范围是1x1度，图片分辨率3601x3601
 * 将每块数据在划分成16x16的小块，即每一小块的范围是0.0625x0.0625
 * GDAL每次读取一小块数据，并放入LRU缓存队列
 *
 */

#include "gdem.h"
#include "unsuck.hpp"
#include "logger.h"

#include <execution>
#include <algorithm>

#include <gdal_priv.h>

using namespace std;

GdemPool::GdemPool()
{
    GDALAllRegister();
    default_projection = R"(GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]])";
}

GdemPool::~GdemPool()
{
}

void GdemPool::init(std::vector<std::string> sources, int &max_lod, int tile_size, State &state)
{
    cout << endl;
    cout << "=======================================" << endl;
    cout << "=== init gdem                           " << endl;
    cout << "=======================================" << endl;

    auto tStart = now();

    state.name = "init";
    state.currentPass = 1;
    state.tilesProcessed = 0;
    state.duration = 0;

    double min_resolution = 1.0 / 3600.0;
    double resolution_at_lod0 = 180.0 / (tile_size - 1.0);
    int lod = 0;
    double resolution = resolution_at_lod0;
    while (resolution > min_resolution)
    {
        lod++;
        resolution /= 2.0;
    }

    if (max_lod < 0 || max_lod > lod)
    {
        max_lod = lod;
    }

    vector<string> expanded;
    while (sources.size() > 0)
    {
        auto path = sources.back();
        sources.pop_back();

        if (fs::is_directory(path))
        {
            for (auto &entry : fs::directory_iterator(path))
            {
                string str = entry.path().string();
                sources.push_back(str);
            }
        }
        else if (fs::is_regular_file(path))
        {
            if (iEndsWith(path, "dem.tif"))
            {
                expanded.push_back(path);
            }
        }
    }

    state.tilesTotal = expanded.size();
    double lastReport = now();
    int tilesProcessed = 0;
    mutex mtx;
    auto parallel = std::execution::par;
    for_each(
        parallel, expanded.begin(), expanded.end(), [&](string path)
        {
            GDALDataset *poDataset = static_cast<GDALDataset *>(GDALOpen(path.c_str(), GA_ReadOnly));
            if (!poDataset)
            {
                logger::WARN(path + " cannot be opened.");
                return;
            }

            int w = poDataset->GetRasterXSize();
            int h = poDataset->GetRasterYSize();
            int bpp = poDataset->GetRasterCount();
            GDALClose(poDataset);

            if (w == 3601 && h == 3601)
            {
                string file_name = fs::path(path).stem().string();
                size_t index1 = file_name.find('_');
                if (index1 < 0)
                {
                    logger::WARN(path + "is not a valid gdem tif");
                }
                else
                {
                    char lat_char = file_name.at(index1 + 1);
                    string lat_str = file_name.substr(index1 + 2, 2);
                    char lon_char = file_name.at(index1 + 4);
                    string lon_str = file_name.substr(index1 + 5, 3);

                    int ilat = ::atoi(lat_str.c_str());
                    int ilon = ::atoi(lon_str.c_str());
                    if (lat_char == 'S')
                        ilat = -ilat;
                    if (lon_char == 'W')
                        ilon = -ilon;

                    double bmin[2] = {(double)ilon, (double)ilat};
                    double bmax[2] = {ilon + 1.0, ilat + 1.0};

                    ilat += 90;
                    ilon += 180;
                    int key = ilat * 360 + ilon;

                    lock_guard<mutex> lock(mtx);
                    tile_map[key] = path;
                    tile_tree.Insert(bmin,bmax,key);
                    
                    tilesProcessed = tilesProcessed + 1;
                    if (now() - lastReport > 1.0)
                    {
                        state.tilesProcessed = tilesProcessed;
                        state.duration = now() - tStart;

                        lastReport = now();
                    }
                }
            }
            else
            {
                logger::WARN(path + "is not a valid gdem tif");
            } }

    );

    double duration = now() - tStart;
    state.values["duration(init)"] = formatNumber(duration, 3);
}

double GdemPool::getElevation(double lon, double lat, State &state)
{
    int ilon = (int)(lon + 180.0);
    int ilat = (int)(lat + 90.0);
    int key = ilat * 360 + ilon;

    if (tile_map.find(key) == tile_map.end())
    {
        return NODATA;
    }

    int ilon_block = (int)(lon * 16.0 + 180.0 * 16.0);
    int ilat_block = (int)(lat * 16.0 + 90.0 * 16.0);
    int key_block = ilat_block * 360 * 16 + ilon_block;

    shared_ptr<DEMTileBlock> pTileBlock;
    if (!tile_cache.tryGet(key_block, pTileBlock))
    {
        GDALDataset *poDataset = static_cast<GDALDataset *>(GDALOpen(tile_map[key].c_str(), GA_ReadOnly));
        if (!poDataset)
        {
            logger::ERROR(tile_map[key] + " cannot be opened.");
            exit(1);
        }

        pTileBlock = make_shared<DEMTileBlock>(ilon_block * 0.0625 - 180.0, ilat_block * 0.0625 - 90.0);
        pTileBlock->data = new int16_t[226 * 226];

        auto poBand = poDataset->GetRasterBand(1);
        auto dataType = poBand->GetRasterDataType();

        int xSize = poBand->GetXSize();
        int ySize = poBand->GetYSize();
        int nXBlockSize, nYBlockSize; // should be 256
        poBand->GetBlockSize(&nXBlockSize, &nYBlockSize);
        if (nXBlockSize < 226 || nYBlockSize < 226)
        {
            logger::ERROR("Block size of " + tile_map[key] + " is less than 226.");
            exit(1);
        }

        // int nXBlocks = (poBand->GetXSize() + nXBlockSize - 1) / nXBlockSize;
        // int nYBlocks = (poBand->GetYSize() + nYBlockSize - 1) / nYBlockSize;

        // int x = 0, y = 0;
        // int16_t *pabyData = new int16_t[nXBlockSize * nYBlockSize];
        // for (int iYBlock = 0; iYBlock < nYBlocks; iYBlock++)
        // {
        //     x = 0;
        //     for (int iXBlock = 0; iXBlock < nXBlocks; iXBlock++)
        //     {
        //         poBand->ReadBlock(iXBlock, iYBlock, pabyData);

        //         int nXValid = nXBlockSize, nYValid = nYBlockSize;
        //         if (x + nXBlockSize > xSize)
        //             nXValid = xSize - x;
        //         if (y + nYBlockSize > ySize)
        //             nYValid = ySize - y;

        //         for (int iY = 0; iY < nYValid; iY++)
        //         {
        //             for (int iX = 0; iX < nXValid; iX++)
        //             {
        //                 pTile->data[(y + iY) * ySize + (x + iX)] = pabyData[iX + iY * nXBlockSize];
        //             }
        //         }
        //         x += nXBlockSize;
        //     }
        //     y += nYBlockSize;
        // }
        // delete pabyData;
        // pabyData = nullptr;

        // auto code = pRasterBand->ReadBlock(3601, 3601, pTile->data);
        // if (code != CPLErr::CE_None)
        // {
        //     logger::ERROR(tile_map[key] + " cannot be opened.");
        //     exit(1);
        // }

        int xOffset = (ilon_block % 16) * 225;
        int yOffset = (15 - (ilat_block % 16)) * 225; // ilat_block是从左下角开始的，yOffset是从图像左上角起始
        auto code = poBand->RasterIO(GDALRWFlag::GF_Read, xOffset, yOffset, 226, 226, pTileBlock->data, 226, 226, dataType, 0, 0);
        if (code != CPLErr::CE_None)
        {
            logger::ERROR(tile_map[key] + " cannot be opened.");
            exit(1);
        }
        GDALClose(poDataset);

        tile_cache.insert(key_block, pTileBlock, state);
    }

    double unit_col = (lon - pTileBlock->west) * 16.0;
    double unit_row = (pTileBlock->south + 0.0625 - lat) * 16.0;
    int col = (int)(225.0 * unit_col + 0.5);
    int row = (int)(225.0 * unit_row + 0.5);
    int16_t ele = pTileBlock->data[row * 226 + col];
    if (ele <= NODATA)
    {
        logger::WARN("found nodata at " + tile_map[key]);
    }
    return ele;
}

bool GdemPool::contains(double west, double south, double east, double north)
{
    double bmin[2] = {west, south};
    double bmax[2] = {east, north};

    int n = tile_tree.Search(bmin, bmax, nullptr);
    if (n > 0)
        return true;
    else
        return false;
}

void GdemPool::makeElevation(double west, double south, double east, double north, int width, int height, int16_t *data, State &state)
{
    double xStep = (east - west) / (width - 1.0);
    double yStep = (north - south) / (height - 1.0);
    for (int y = 0; y < height; y++)
    {
        for (int x = 0; x < width; x++)
        {
            double lon = west + x * xStep;
            double lat = north - y * yStep;
            double ele = getElevation(lon, lat, state);
            if (ele <= NODATA)
                ele = 0.0;

            data[y * width + x] = (int16_t)ele;
        }
    }
}

void GdemPool::makeElevationImage(double west, double south, double east, double north,
                                  int width, int height, string format, string type, string path, State &state)
{
    if (fs::exists(path))
        return;

    if (!contains(west, south, east, north))
        return;

    if (format == "grey")
    {
        int16_t *data = new int16_t[width * height];
        makeElevation(west, south, east, north, width, height, data, state);

        if (icompare(type, "png"))
        {
            GDALDriver *pDriverMEM = GetGDALDriverManager()->GetDriverByName("MEM");
            GDALDataset *pOutMEMDataset = pDriverMEM->Create("", width, height, 1, GDT_UInt16, NULL);
            if (!pOutMEMDataset)
            {
                logger::ERROR("cannot create MEM image.");
                return;
            }
            pOutMEMDataset->RasterIO(GF_Write, 0, 0, width, height, (void *)data, width, height,
                                     GDT_UInt16, 1, nullptr, 0, 0, 0);

            // 以创建复制的方式，生成png文件
            GDALDriver *pDriverPNG = GetGDALDriverManager()->GetDriverByName("PNG");
            // GDALDriver *pDriverPNG = GetGDALDriverManager()->GetDriverByName("JPEG");
            GDALDataset *tile = pDriverPNG->CreateCopy(path.c_str(), pOutMEMDataset, TRUE, 0, 0, 0);
            if (!tile)
            {
                logger::ERROR("cannot create PNG image.");
                return;
            }

            GDALClose(pOutMEMDataset);
            pOutMEMDataset = nullptr;

            GDALClose(tile);
            tile = nullptr;
        }
        else if (icompare(type, "tif"))
        {
            GDALDriver *pDriverTIF = GetGDALDriverManager()->GetDriverByName("GTiff");
            GDALDataset *pOutTIFDataset = pDriverTIF->Create(path.c_str(), width, height, 1, GDT_Int16, NULL);
            if (!pOutTIFDataset)
            {
                logger::ERROR("cannot create TIF image.");
                return;
            }
            pOutTIFDataset->RasterIO(GF_Write, 0, 0, width, height, (void *)data, width, height,
                                     GDT_Int16, 1, nullptr, 0, 0, 0);
            double xResolution = (east - west) / (width - 1);
            double yResolution = (south - north) / (height - 1);
            double geoTransform[6] = {
                west - xResolution * 0.5,
                xResolution,
                0,
                north - yResolution * 0.5,
                0,
                yResolution};
            pOutTIFDataset->SetGeoTransform(geoTransform);
            pOutTIFDataset->SetProjection(default_projection.c_str());

            GDALClose(pOutTIFDataset);
            pOutTIFDataset = nullptr;
        }
        else
        {
            logger::WARN("unsupported type, [png, tif] suppported.");
        }

        delete data;
        data = nullptr;
    }
}

void GdemPool::makeElevationImage(int z, int x, int y, int width, int height,
                                  string format, string type, string out_dir, State &state)
{
    double step = 180.0 / (1 << z);
    double west = -180.0 + x * step;
    double east = west + step;
    double north = 90 - y * step;
    double south = north - step;

    string path = out_dir + "/" + formatNumber(z) + "/" + formatNumber(x) + "/" + formatNumber(y) + "." + type;
    makeElevationImage(west, south, east, north, width, height, format, type, path, state);
}

void GdemPool::makeLodImage(int z, int x, int y, int width, int height,
                            string format, string type, string out_dir, State &state)
{
    string path = out_dir + "/" + formatNumber(z) + "/" + formatNumber(x) + "/" + formatNumber(y) + "." + type;
    if (fs::exists(path))
        return;

    /**
     *  | 00 10 |
     *  | 01 11 |
     */
    string path00 = out_dir + "/" + formatNumber(z + 1) + "/" + formatNumber(x * 2) + "/" + formatNumber(y * 2) + "." + type;
    string path01 = out_dir + "/" + formatNumber(z + 1) + "/" + formatNumber(x * 2) + "/" + formatNumber(y * 2 + 1) + "." + type;
    string path10 = out_dir + "/" + formatNumber(z + 1) + "/" + formatNumber(x * 2 + 1) + "/" + formatNumber(y * 2) + "." + type;
    string path11 = out_dir + "/" + formatNumber(z + 1) + "/" + formatNumber(x * 2 + 1) + "/" + formatNumber(y * 2 + 1) + "." + type;

    bool exist00 = fs::exists(path00);
    bool exist01 = fs::exists(path01);
    bool exist10 = fs::exists(path10);
    bool exist11 = fs::exists(path11);
    if (!exist00 && !exist01 && !exist10 && !exist11)
        return;

    if (format == "grey")
    {
        int16_t *data = new int16_t[width * height];
        for (int i = 0; i < width * height; i++)
            data[i] = 0;

        int subwidth = (int)(width / 2 + 1);
        int subheight = (int)(height / 2 + 1);
        int16_t *subdata = new int16_t[subwidth * subheight];

        if (exist00)
        {
            GDALDataset *poDataset = static_cast<GDALDataset *>(GDALOpen(path00.c_str(), GA_ReadOnly));
            if (!poDataset)
            {
                logger::WARN(path00 + " cannot be opened.");
                logger::WARN("try to recreate " + path00);
                fs::remove(path00);
                makeElevationImage(z + 1, x * 2, y * 2, width, height, format, type, out_dir, state);
                poDataset = static_cast<GDALDataset *>(GDALOpen(path00.c_str(), GA_ReadOnly));
            }

            auto code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                            subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
            if (code != CPLErr::CE_None)
            {
                logger::WARN(path00 + " cannot be opened.");
                logger::WARN("try to recreate " + path00);
                GDALClose(poDataset);
                fs::remove(path00);
                makeElevationImage(z + 1, x * 2, y * 2, width, height, format, type, out_dir, state);
                poDataset = static_cast<GDALDataset *>(GDALOpen(path00.c_str(), GA_ReadOnly));
                code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                           subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
            }

            if (code != CPLErr::CE_None)
                return;

            for (int y = 0; y < subheight; y++)
            {
                for (int x = 0; x < subwidth; x++)
                {
                    data[y * width + x] = subdata[y * subwidth + x];
                }
            }
            GDALClose(poDataset);
        }

        if (exist01)
        {
            GDALDataset *poDataset = static_cast<GDALDataset *>(GDALOpen(path01.c_str(), GA_ReadOnly));
            if (!poDataset)
            {
                logger::WARN(path01 + " cannot be opened.");
                logger::WARN("try to recreate " + path01);
                fs::remove(path01);
                makeElevationImage(z + 1, x * 2, y * 2 + 1, width, height, format, type, out_dir, state);
                poDataset = static_cast<GDALDataset *>(GDALOpen(path01.c_str(), GA_ReadOnly));
            }

            auto code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                            subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
            if (code != CPLErr::CE_None)
            {
                logger::WARN(path01 + " cannot be opened.");
                logger::WARN("try to recreate " + path01);
                GDALClose(poDataset);
                fs::remove(path01);
                makeElevationImage(z + 1, x * 2, y * 2 + 1, width, height, format, type, out_dir, state);
                poDataset = static_cast<GDALDataset *>(GDALOpen(path01.c_str(), GA_ReadOnly));
                code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                           subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
            }

            if (code != CPLErr::CE_None)
                return;

            for (int y = 0; y < subheight; y++)
            {
                for (int x = 0; x < subwidth; x++)
                {
                    data[(height - subheight + y) * width + x] = subdata[y * subwidth + x];
                }
            }
            GDALClose(poDataset);
        }

        if (exist10)
        {
            GDALDataset *poDataset = static_cast<GDALDataset *>(GDALOpen(path10.c_str(), GA_ReadOnly));
            if (!poDataset)
            {
                logger::WARN(path10 + " cannot be opened.");
                logger::WARN("try to recreate " + path10);
                fs::remove(path10);
                makeElevationImage(z + 1, x * 2 + 1, y * 2, width, height, format, type, out_dir, state);
                poDataset = static_cast<GDALDataset *>(GDALOpen(path10.c_str(), GA_ReadOnly));
            }

            auto code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                            subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
            if (code != CPLErr::CE_None)
            {
                logger::WARN(path10 + " cannot be opened.");
                logger::WARN("try to recreate " + path10);
                GDALClose(poDataset);
                fs::remove(path10);
                makeElevationImage(z + 1, x * 2 + 1, y * 2, width, height, format, type, out_dir, state);
                poDataset = static_cast<GDALDataset *>(GDALOpen(path10.c_str(), GA_ReadOnly));
                code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                           subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
            }

            if (code != CPLErr::CE_None)
                return;

            for (int y = 0; y < subheight; y++)
            {
                for (int x = 0; x < subwidth; x++)
                {
                    data[y * width + width - subwidth + x] = subdata[y * subwidth + x];
                }
            }
            GDALClose(poDataset);
        }

        if (exist11)
        {
            GDALDataset *poDataset = static_cast<GDALDataset *>(GDALOpen(path11.c_str(), GA_ReadOnly));
            if (!poDataset)
            {
                logger::WARN(path11 + " cannot be opened.");
                logger::WARN("try to recreate " + path11);
                fs::remove(path11);
                makeElevationImage(z + 1, x * 2 + 1, y * 2 + 1, width, height, format, type, out_dir, state);
                poDataset = static_cast<GDALDataset *>(GDALOpen(path11.c_str(), GA_ReadOnly));
            }

            auto code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                            subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
            if (code != CPLErr::CE_None)
            {
                logger::WARN(path11 + " cannot be opened.");
                logger::WARN("try to recreate " + path11);
                GDALClose(poDataset);
                fs::remove(path11);
                makeElevationImage(z + 1, x * 2 + 1, y * 2 + 1, width, height, format, type, out_dir, state);
                poDataset = static_cast<GDALDataset *>(GDALOpen(path11.c_str(), GA_ReadOnly));
                code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                           subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
            }

            if (code != CPLErr::CE_None)
                return;

            for (int y = 0; y < subheight; y++)
            {
                for (int x = 0; x < subwidth; x++)
                {
                    data[(height - subheight + y) * width + width - subwidth + x] = subdata[y * subwidth + x];
                }
            }
            GDALClose(poDataset);
        }

        delete subdata;
        subdata = nullptr;

        if (icompare(type, "png"))
        {
            GDALDriver *pDriverMEM = GetGDALDriverManager()->GetDriverByName("MEM");
            GDALDataset *pOutMEMDataset = pDriverMEM->Create("", width, height, 1, GDT_UInt16, NULL);
            if (!pOutMEMDataset)
            {
                logger::ERROR("cannot create MEM image.");
                return;
            }
            pOutMEMDataset->RasterIO(GF_Write, 0, 0, width, height, (void *)data, width, height,
                                     GDT_UInt16, 1, nullptr, 0, 0, 0);

            // 以创建复制的方式，生成png文件
            GDALDriver *pDriverPNG = GetGDALDriverManager()->GetDriverByName("PNG");
            // GDALDriver *pDriverPNG = GetGDALDriverManager()->GetDriverByName("JPEG");
            GDALDataset *tile = pDriverPNG->CreateCopy(path.c_str(), pOutMEMDataset, TRUE, 0, 0, 0);
            if (!tile)
            {
                logger::ERROR("cannot create PNG image.");
                return;
            }

            GDALClose(pOutMEMDataset);
            pOutMEMDataset = nullptr;

            GDALClose(tile);
            tile = nullptr;
        }
        else if (icompare(type, "tif"))
        {
            GDALDriver *pDriverTIF = GetGDALDriverManager()->GetDriverByName("GTiff");
            GDALDataset *pOutTIFDataset = pDriverTIF->Create(path.c_str(), width, height, 1, GDT_Int16, NULL);
            if (!pOutTIFDataset)
            {
                logger::ERROR("cannot create TIF image.");
                return;
            }
            pOutTIFDataset->RasterIO(GF_Write, 0, 0, width, height, (void *)data, width, height,
                                     GDT_Int16, 1, nullptr, 0, 0, 0);

            double step = 180.0 / (1 << z);
            double west = -180.0 + x * step;
            double east = west + step;
            double north = 90 - y * step;
            double south = north - step;
            double xResolution = (east - west) / (width - 1);
            double yResolution = (south - north) / (height - 1);
            double geoTransform[6] = {
                west - xResolution * 0.5,
                xResolution,
                0,
                north - yResolution * 0.5,
                0,
                yResolution};
            pOutTIFDataset->SetGeoTransform(geoTransform);
            pOutTIFDataset->SetProjection(default_projection.c_str());

            GDALClose(pOutTIFDataset);
            pOutTIFDataset = nullptr;
        }
        else
        {
            logger::ERROR("unsupported type, [png, tif] suppported.");
        }

        delete data;
        data = nullptr;
    }
}

void GdemPool::makeNullImage(int width, int height, std::string format, std::string out_dir)
{
    try
    {
        string path = out_dir + "/null.png";

        if (format == "grey")
        {
            int16_t *data = new int16_t[width * height];
            for (int i = 0; i < width * height; i++)
                data[i] = 0;

            GDALDriver *pDriverMEM = GetGDALDriverManager()->GetDriverByName("MEM");
            GDALDataset *pOutMEMDataset = pDriverMEM->Create("", width, height, 1, GDT_UInt16, NULL);
            if (!pOutMEMDataset)
            {
                logger::ERROR("cannot create MEM image.");
                return;
            }
            pOutMEMDataset->RasterIO(GF_Write, 0, 0, width, height, (void *)data, width, height,
                                     GDT_UInt16, 1, nullptr, 0, 0, 0);

            // 以创建复制的方式，生成png文件
            GDALDriver *pDriverPNG = GetGDALDriverManager()->GetDriverByName("PNG");
            // GDALDriver *pDriverPNG = GetGDALDriverManager()->GetDriverByName("JPEG");
            GDALDataset *tile = pDriverPNG->CreateCopy(path.c_str(), pOutMEMDataset, TRUE, 0, 0, 0);
            if (!tile)
            {
                logger::ERROR("cannot create PNG image.");
                return;
            }

            GDALClose(pOutMEMDataset);
            pOutMEMDataset = nullptr;

            GDALClose(tile);
            tile = nullptr;

            delete data;
            data = nullptr;
        }
        else if (format == "rgba")
        {
            GDALDriver *pDriverMEM = GetGDALDriverManager()->GetDriverByName("MEM");
            GDALDataset *pOutMEMDataset = pDriverMEM->Create("", width, height, 1, GDT_Byte, NULL);
            if (!pOutMEMDataset)
            {
                logger::ERROR("cannot create MEM image.");
                return;
            }
            pOutMEMDataset->GetRasterBand(1)->Fill(0.0);

            GDALColorEntry colorEntry{0};
            GDALColorTable colorTable(GPI_RGB);
            colorTable.SetColorEntry(0, &colorEntry);
            pOutMEMDataset->GetRasterBand(1)->SetColorTable(&colorTable);
            pOutMEMDataset->GetRasterBand(1)->SetColorInterpretation(GCI_PaletteIndex);

            // 以创建复制的方式，生成png文件
            GDALDriver *pDriverPNG = GetGDALDriverManager()->GetDriverByName("PNG");
            // GDALDriver *pDriverPNG = GetGDALDriverManager()->GetDriverByName("JPEG");
            GDALDataset *tile = pDriverPNG->CreateCopy(path.c_str(), pOutMEMDataset, TRUE, 0, 0, 0);
            if (!tile)
            {
                logger::ERROR("cannot create PNG image.");
                return;
            }

            GDALClose(pOutMEMDataset);
            pOutMEMDataset = nullptr;

            GDALClose(tile);
            tile = nullptr;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << '\n';
    }
}

void GdemPool::repairImage(int z, int x, int y, int width, int height,
                           std::string format, std::string type, std::string out_dir, State &state)
{
    lock_guard<mutex> lock(repair_mutex);

    string path = out_dir + "/" + formatNumber(z) + "/" + formatNumber(x) + "/" + formatNumber(y) + "." + type;
    GDALDataset *poDataset = static_cast<GDALDataset *>(GDALOpen(path.c_str(), GA_ReadOnly));
    if (!poDataset)
    {
        logger::WARN(path + " cannot be opened.");
        logger::WARN("try to recreate " + path);
        fs::remove(path);
        makeElevationImage(z, x, y, width, height, format, type, out_dir, state);
        poDataset = static_cast<GDALDataset *>(GDALOpen(path.c_str(), GA_ReadOnly));
    }

    width = poDataset->GetRasterXSize();
    height = poDataset->GetRasterYSize();

    int subwidth = (int)(width / 2 + 1);
    int subheight = (int)(height / 2 + 1);
    int16_t *subdata = new int16_t[subwidth * subheight];
    auto code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                    subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
    if (code != CPLErr::CE_None)
    {
        logger::WARN(path + " cannot be opened.");
        logger::WARN("try to recreate " + path);
        GDALClose(poDataset);
        fs::remove(path);
        makeElevationImage(z, x, y, width, height, format, type, out_dir, state);
        poDataset = static_cast<GDALDataset *>(GDALOpen(path.c_str(), GA_ReadOnly));
        code = poDataset->RasterIO(GDALRWFlag::GF_Read, 0, 0, width, height,
                                   subdata, subwidth, subheight, GDT_Int16, 1, nullptr, 0, 0, 0);
    }

    if (code != CPLErr::CE_None)
        return;

    GDALClose(poDataset);
}