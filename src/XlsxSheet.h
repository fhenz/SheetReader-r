#pragma once

#include <string>
#include <map>
#include <atomic>
#include <list>
#include <vector>

#include "miniz/miniz.h"

int fileIndex(mz_zip_archive* archive, const char* file_name);
unsigned long strtoul(const char* start, const unsigned long length);

constexpr const size_t BUFFER_SIZE = 32768;

enum class CellType : unsigned char {
    T_NONE = 0,
    T_NUMERIC = 1,
    T_STRING_REF = 2,
    T_STRING = 3,
    T_STRING_INLINE = 4,
    T_BOOLEAN = 5,
    T_ERROR = 6,
    T_DATE = 7
};

struct LocationInfo {
    unsigned long buffer;
    unsigned long cell;
    unsigned long column;
    unsigned long row;
};

struct XlsxCell {
    union {
        double real;
        unsigned long long integer;
        bool boolean;
    } data;
};

class XlsxFile;
class XlsxSheet {
public:
    unsigned long mSkipRows = 0;
    unsigned long mSkipColumns = 0;
public:
    XlsxFile& mParentFile;

    mz_zip_archive* mFile;
    int mArchiveIndex;

    std::vector<std::list<std::pair<std::vector<XlsxCell>, std::vector<CellType>>>> mCells;
    std::vector<std::vector<LocationInfo>> mLocationInfos;

    bool mHeaders;
    std::pair<unsigned long, unsigned long> mDimension;

    XlsxSheet(XlsxSheet&& sheet);

    XlsxSheet(XlsxFile& parentFile, mz_zip_archive* file, int archiveIndex);
    
    bool interleaved(const int skipRows, const int skipColumns, const int numThreads);
    template<std::size_t num_buffers>
    void interleavedFunc(size_t numThreads, const size_t threadId, std::array<unsigned char*, num_buffers>& buffers, const size_t bufferSize, const std::atomic_size_t& writeIndex, const std::atomic_bool& finishedWriting, std::vector<std::atomic_size_t>& readIndexes, const std::atomic_bool& terminate);
};
