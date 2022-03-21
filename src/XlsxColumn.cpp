#include "XlsxColumn.h"

#include "XlsxSheet.h"
#include "XlsxFile.h"

XlsxColumn::XlsxColumn(const XlsxSheet& parent_sheet)
    : mParentSheet(parent_sheet)
    , mHeader()
    , mHeaderType(CellType::T_NONE)
    , mHasHeader(false)
    , mType(CellType::T_NONE)
{
}

bool XlsxColumn::placeCell(const cell cll, const CellType type, const unsigned long row) {
    if (row == 1 && mParentSheet.mHeaders) {
        mHeader = cll;
        mHeaderType = type;
        mHasHeader = true;
        return true;
    }
    const unsigned long long adjRow = (mParentSheet.mHeaders ? row-1 : row);
    if (mCells.size() < adjRow) {
        mCells.resize(adjRow);
        mTypes.resize(adjRow, CellType::T_NONE);
    }
    if (mTypes[adjRow-1] != CellType::T_NONE) return false;
    mCells[adjRow-1] = cll;
    mTypes[adjRow-1] = type;
    return true;
}

void XlsxColumn::reserve(const unsigned long size) {
    if (mCells.size() < size) {
        mCells.resize(size);
        mTypes.resize(size, CellType::T_NONE);
    }
}

void XlsxColumn::clear() {
    // Force reallocation
    std::vector<cell>().swap(mCells);
    std::vector<CellType>().swap(mTypes);
}

XlsxColumn& XlsxColumn::operator=(const XlsxColumn& column) {
    mHeader = column.mHeader;
    mHasHeader = column.mHasHeader;
    mCells = column.mCells;
    mTypes = column.mTypes;
    mType = column.mType;
    return *this;
}
