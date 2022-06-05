#pragma once

#include <vector>

#include <Rcpp.h>

class XlsxSheet;
class XlsxColumn {
public:
    union cell {
        double real;
        unsigned long long integer;
        bool boolean;
    };
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

    const XlsxSheet& mParentSheet;

    cell mHeader;
    CellType mHeaderType;
    bool mHasHeader;
    std::vector<cell> mCells;
    std::vector<CellType> mTypes;
    CellType mType;

    XlsxColumn(const XlsxSheet& parentSheet);
    bool placeCell(const cell cell, const CellType type, const unsigned long row);
    void reserve(const unsigned long size);
    void clear();

    XlsxColumn& operator=(const XlsxColumn& column);
};
