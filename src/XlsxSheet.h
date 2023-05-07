#pragma once

#include <string>
#include <map>
#include <atomic>
#include <list>

#include "miniz/miniz.h"

#include "XlsxColumn.h"

int fileIndex(mz_zip_archive* archive, const char* file_name);
unsigned long strtoul(const char* start, const unsigned long length);

constexpr const size_t BUFFER_SIZE = 32768;

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
    //XlsxColumn::CellType type;
};

class XlsxFile;
class XlsxSheet {
    unsigned long mSkipRows = 0;
    unsigned long mSkipColumns = 0;
public:
    XlsxFile& mParentFile;

    mz_zip_archive* mFile;
    int mArchiveIndex;

    std::vector<XlsxColumn> mColumns;
    std::atomic_bool mReserved;
    std::atomic_bool mFallbackSingle;
    std::vector<std::list<std::pair<std::vector<XlsxCell>, std::vector<XlsxColumn::CellType>>>> mCells;
    std::vector<std::vector<LocationInfo>> mLocationInfos;

    bool mHeaders;

    std::pair<unsigned long, unsigned long> mDimension;

    XlsxSheet(XlsxSheet&& sheet);

    XlsxSheet(XlsxFile& parentFile, mz_zip_archive* file, int archiveIndex);


    bool placeCell(const double value, const XlsxColumn::CellType type, const unsigned long rowNumber, const unsigned long columnNumber);
    bool placeCell(const unsigned long long value, const XlsxColumn::CellType type, const unsigned long rowNumber, const unsigned long columnNumber);
    void reserve(const unsigned long columns, const unsigned long rows, bool resize = false);

    bool consecutive(const int skipRows, const int skipColumns, int numThreads);
    void consecutiveFunc(const size_t threadId, const unsigned char* const buffer, const size_t length, const size_t maxLength);
    
    bool interleaved(const int skipRows, const int skipColumns, const int numThreads);
    template<std::size_t num_buffers>
    void interleavedFunc(size_t numThreads, const size_t threadId, std::array<unsigned char*, num_buffers>& buffers, const size_t bufferSize, const std::atomic_size_t& writeIndex, const std::atomic_bool& finishedWriting, std::vector<std::atomic_size_t>& readIndexes);
};
