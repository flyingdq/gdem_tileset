#include "unsuck.hpp"

#include <iostream>
using namespace std;

void rename(std::string path)
{
    if(fs::is_directory(path)) 
    {
        if(path.find("'") != -1)
        {
            string newPath = stringReplaceAll(path, "'", "");
            fs::rename(path, newPath);
            path = newPath;
        }
        
        for (auto &entry : fs::directory_iterator(path))
        {
            string str = entry.path().string();
            rename(str);
        }
    } 
    else if (fs::is_regular_file(path))
    {
        if(path.find("'") != -1)
        {
            string newPath = stringReplaceAll(path, "'", "");
            fs::rename(path, newPath);
        }
    }
}

int main(int argc, char **argv)
{
    double tStart = now();

    rename("D:\\GDEM_TIF_tileset_0-12");

    double duration = now() - tStart;

    cout << "duration:              " << formatNumber(duration, 3) << "s" << endl;

    return 0;
}