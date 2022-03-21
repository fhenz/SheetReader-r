#include "XlsxSheet.h"

#include <map>
#include <thread>
#include <atomic>
#include <array>

#include "XlsxFile.h"

#include "fast_double_parser/fast_double_parser.h"
#include "libdeflate/lib/libdeflate.h"
#include "parsing.h"

unsigned long strtoul(const char* start, const unsigned long length) {
    unsigned long val = 0;
    unsigned long offset = 0;
    while(offset < length) {
        val = val * 10 + start[offset++] - '0';
    }
    return val;
}

unsigned long alphatoul(const char* start, const unsigned long length) {
    unsigned long val = 0;
    unsigned long offset = 0;
    while(offset < length) {
        val = val * 26 + start[offset++] - 64;
    }
    return val;
}

// Extracts the unsigned number from the buffer at the given offset with no checks whatsoever.
// Also assumes the first character is a digit
inline size_t extractUnsigned(const char* buffer, const size_t baseOffset, const size_t bufferSize) {
    size_t offset = 0;
    size_t d;
    size_t n = buffer[baseOffset] - '0';

    while (baseOffset + offset < bufferSize && (d = (buffer[baseOffset + ++offset] - '0')) <= 9) {
        n = n * 10 + d;
    }

    return n;
}

XlsxSheet::XlsxSheet(XlsxSheet&& sheet)
    : mParentFile(sheet.mParentFile)
    , mFile(sheet.mFile)
    , mColumns(sheet.mColumns)
    , mReserved(sheet.mReserved.load())
    , mFallbackSingle(sheet.mFallbackSingle.load())
    , mHeaders(sheet.mHeaders)
{
}

XlsxSheet::XlsxSheet(XlsxFile& parentFile, mz_zip_archive* file, int archiveIndex)
    : mParentFile(parentFile)
    , mFile(file)
    , mArchiveIndex(archiveIndex)
    , mReserved(false)
    , mFallbackSingle(false)
    , mHeaders(false)
{
}

bool XlsxSheet::placeCell(const double value, const XlsxColumn::CellType type, const unsigned long rowNumber, const unsigned long columnNumber) {
    if (rowNumber <= mSkipRows || columnNumber <= mSkipColumns) return true;
    if (mColumns.size() < columnNumber - mSkipColumns) {
        mColumns.resize(columnNumber - mSkipColumns, XlsxColumn(*this));
    }
    XlsxColumn::cell cll;
    cll.real = value;
    return mColumns[columnNumber - 1 - mSkipColumns].placeCell(cll, type, rowNumber - mSkipRows);
}

bool XlsxSheet::placeCell(const unsigned long long value, const XlsxColumn::CellType type, const unsigned long rowNumber, const unsigned long columnNumber) {
    if (rowNumber <= mSkipRows || columnNumber <= mSkipColumns) return true;
    if (mColumns.size() < columnNumber - mSkipColumns) {
        mColumns.resize(columnNumber - mSkipColumns, XlsxColumn(*this));
    }
    XlsxColumn::cell cll;
    cll.integer = value;
    return mColumns[columnNumber - 1 - mSkipColumns].placeCell(cll, type, rowNumber - mSkipRows);
}

void XlsxSheet::reserve(const unsigned long columns, const unsigned long rows) {
    mColumns.reserve(columns - mSkipColumns);
    for (size_t i = 0; i < mColumns.capacity(); ++i) {
        mColumns.emplace_back(*this);
        mColumns[i].reserve(rows - mHeaders - mSkipRows);
    }
}

bool XlsxSheet::consecutive(const int skipRows, const int skipColumns, int numThreads) {
    size_t fileOffset = 0;
    size_t compSize = 0;
    size_t uncompSize = 0;
    if (!mParentFile.getFile(mArchiveIndex, fileOffset, compSize, uncompSize)) {
        throw std::runtime_error("Unable to retrieve sheet");
    }
    mSkipRows = skipRows;
    mSkipColumns = skipColumns;

    unsigned char* readBuffer = new unsigned char[compSize];
    const auto nread = mFile->m_pRead(mFile->m_pIO_opaque, fileOffset, readBuffer, compSize);
    if (nread != compSize) {
        delete[] readBuffer;
        throw std::runtime_error("Failed to read sheet from archive");
    }

    struct libdeflate_decompressor *decompressor = libdeflate_alloc_decompressor();

    unsigned char* buffer = new unsigned char[uncompSize];

    const auto result = libdeflate_deflate_decompress(decompressor, readBuffer, compSize, buffer, uncompSize, nullptr);

    libdeflate_free_decompressor(decompressor);

    delete[] readBuffer;

    if (result != 0) {
        delete[] buffer;
        throw std::runtime_error("Failed to decompress sheet");
    }

    // limit the number of threads based on uncompressed size
    // (there shouldn't be a problem with a lot of threads for a small file but its unnecessary)
    const int maxThreads = static_cast<int>(uncompSize / BUFFER_SIZE) + 1;
    if (numThreads > maxThreads) numThreads = maxThreads;

    std::vector<std::thread> parseThreads;
    parseThreads.reserve(numThreads - 1);

    for (int i = 0; i < numThreads; ++i) {
        const size_t start = static_cast<size_t>(i * (uncompSize / numThreads));
        const size_t end = i == numThreads - 1 ? uncompSize : static_cast<size_t>((i + 1) * (uncompSize / numThreads));
        if (i == numThreads - 1) {
            // last thread is this thread
            consecutiveFunc(i, buffer + start, end - start, uncompSize - start);
        } else {
            parseThreads.push_back(std::thread(&XlsxSheet::consecutiveFunc, this, i, buffer + start, end - start, uncompSize - start));
        }
    }

    for (size_t i = 0; i < parseThreads.size(); ++i) {
        parseThreads[i].join();
    }

    delete[] buffer;
    return true;
}

void XlsxSheet::consecutiveFunc(const size_t threadId, const unsigned char* const buffer, const size_t length, const size_t maxLength) {
    constexpr size_t cellValueBufferSize = 32768; // Max number of characters in cell is 32767
    char cellValueBuffer[cellValueBufferSize];
    size_t cellValueLength = 0;

    size_t offset = 0;

    ElementParser<1> dimension("dimension", {"ref"}, {AttributeType::RANGE});
    ElementParser<0> sheetData("sheetData", {}, {});
    ElementParser<3> c("c", {"r", "t", "s"}, {AttributeType::LOCATION, AttributeType::TYPE, AttributeType::INDEX});
    ElementParser<0> v("v", {}, {});

    // all other threads wait until data structure has been pre-allocated
    if (threadId > 0) {
        while (!mReserved.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            // also abort all other threads if fallback to single
            if (mFallbackSingle.load()) return;
        }
    }

    while (offset < maxLength) {
        const unsigned char current = buffer[offset];
        ++offset;

        if (!mReserved.load() && !mFallbackSingle.load()) {
            sheetData.process(current);
            dimension.process(current);
            if (dimension.completed() && dimension.hasValue(0)) {
                const auto val = static_cast<const RangeParser&>(dimension.getAttribute(0)).getValue();
                reserve(val.second.first, val.second.second);
                mReserved.store(true);
            } else if (sheetData.inside()) {
                // ! we arrived inside sheet data but without having encountered the dimension tag
                // fallback to single thread parsing
                mFallbackSingle.store(true);
            }
        }
        bool in_c = c.inside();
        c.process(current);
        if (!in_c) continue;
        bool in_v = v.inside();
        v.process(current);
        if (!in_v && v.inside()) continue;
        
        if (c.completed()) {
            XlsxColumn::CellType cellType = XlsxColumn::CellType::T_NUMERIC;
            bool dateStyle = false;
            unsigned long rowNumber = 0;
            unsigned long cellColumn = 0;
            if (c.hasValue(0)) {
                const std::pair<unsigned long, unsigned long> val = static_cast<const LocationParser&>(c.getAttribute(0)).getValue();
                cellColumn = val.first;
                rowNumber = val.second;
            }
            if (c.hasValue(1)) cellType = static_cast<const TypeParser&>(c.getAttribute(1)).getValue();
            if (c.hasValue(2)) dateStyle = mParentFile.isDate(static_cast<const IndexParser&>(c.getAttribute(2)).getValue());

            if (cellType == XlsxColumn::CellType::T_STRING_INLINE) {
                //TODO: inline strings are not in the v element, but in t
                cellValueLength = 0;
                cellValueBuffer[0] = 0;
                continue;
            }
            if (cellValueLength == 0 || (static_cast<int>(cellValueLength) < v.getCloseLength())) {
                // no value
                cellValueLength = 0;
                cellValueBuffer[0] = 0;
                continue;
            }

            cellValueBuffer[cellValueLength - v.getCloseLength() + 1] = 0;
            if (rowNumber == 0 || cellColumn == 0 || cellType == XlsxColumn::CellType::T_NONE) {
                throw std::runtime_error("Error when parsing cell");
            }
            //std::cout << "Thread " << threadId << ": " << rowNumber << " / " << cellColumn << ", " << static_cast<int>(cellType) << " (offset " << offset << "): " << cellValueBuffer << std::endl;
            if (cellType == XlsxColumn::CellType::T_NUMERIC) {
                double value = 0;
                const bool isok = fast_double_parser::parse_number(cellValueBuffer, &value);
                if (!isok) {
                    throw std::runtime_error("Error when parsing number");
                }
                if (dateStyle) {
                    placeCell(mParentFile.toDate(value), XlsxColumn::CellType::T_DATE, rowNumber, cellColumn);
                } else {
                    placeCell(value, cellType, rowNumber, cellColumn);
                }
            } else if (cellType == XlsxColumn::CellType::T_STRING) {
                mParentFile.unescape(cellValueBuffer);
                const unsigned long long stringIndex = mParentFile.addDynamicString(cellValueBuffer);
                placeCell(stringIndex, cellType, rowNumber, cellColumn);
            } else {
                const unsigned long long value = extractUnsigned(cellValueBuffer, 0, cellValueLength);
                placeCell(value, cellType, rowNumber, cellColumn);
            }
            cellValueLength = 0;
            cellValueBuffer[0] = 0;
            if (offset >= length && !mFallbackSingle.load()) break;
            continue;
        }
        if (v.inside()) {
            if (cellValueLength >= cellValueBufferSize) {
                throw std::runtime_error("Exceeded cell value buffer size");
            }
            cellValueBuffer[cellValueLength++] = current;
        }
    }
}

bool XlsxSheet::interleaved(const int skipRows, const int skipColumns, const int numThreads) {
    constexpr const size_t tbufferSize = BUFFER_SIZE + 1;

    constexpr const size_t numBuffers = 1024;
    std::array<unsigned char*, numBuffers> buffers;
    for (size_t i = 0; i < numBuffers; ++i) {
        buffers[i] = new unsigned char[tbufferSize]; //TODO RAII with unique_ptr?
        memset(buffers[i], 0, tbufferSize);
    }
    mSkipRows = skipRows;
    mSkipColumns = skipColumns;

    std::vector<std::atomic_size_t> readIndexes(numThreads);
    std::atomic_size_t writeIndex(numThreads - 1); // gets incremented at the start to num_threads
    std::vector<std::thread> parseThreads;
    parseThreads.reserve(numThreads - 1);

    for (int i = 0; i < numThreads; ++i) {
        readIndexes[i].store(static_cast<size_t>(i));
    }

    const int archiveIndex = mArchiveIndex;
    std::atomic_bool finishedWriting(false);
    bool success = true;
    auto producerThread = std::thread([&]() {
        mz_zip_reader_extract_iter_state* state = mz_zip_reader_extract_iter_new(mFile, archiveIndex, 0);
        if (!state) {
            throw std::runtime_error("Failed to initialize sheet reader state");
        }

        const auto checkIndexes = [](const size_t indexMod, const std::vector<std::atomic_size_t>& readIndexes) {
            for (const auto& index : readIndexes) {
                if (indexMod == (index.load() % numBuffers)) return true;
            }
            return false;
        };

        while (true) {
            const size_t index = writeIndex.load() + 1;
            while (checkIndexes(index % numBuffers, readIndexes)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            if ((!state) || (!state->pZip) || (!state->pZip->m_pState)) {
                finishedWriting.store(true);
                writeIndex.store(index);
                break;
            }

            mz_zip_error err = MZ_ZIP_NO_ERROR;
            const size_t read = mz_zip_reader_extract_iter_read(state, buffers[index % numBuffers], tbufferSize - 1, err);
            if (state->status < 0 || read >= tbufferSize) {
                // decompression error or write error, abort
                writeIndex.store(0);
                finishedWriting.store(true);
                success = false;
                return;
            }
            buffers[index % numBuffers][read] = '\0';
            const bool finished = read == 0 || state->status == TINFL_STATUS_DONE;
            if (finished) finishedWriting.store(true);
            writeIndex.store(index + finished);
            if (finished) break;
        }

        if (state->file_crc32 != state->file_stat.m_crc32) {
            // mismatching crc32
            success = false;
        }
        if (!mz_zip_reader_extract_iter_free(state)) {
            // probably decompression wasn't finished yet
            success = false;
        }
    });

    for (int i = 0; i < numThreads - 1; ++i) {
        parseThreads.push_back(std::thread(&XlsxSheet::interleavedFunc<numBuffers>, this, numThreads, i, std::ref(buffers), tbufferSize, std::cref(writeIndex), std::cref(finishedWriting), std::ref(readIndexes)));
    }

    interleavedFunc<numBuffers>(numThreads, numThreads - 1, buffers, tbufferSize, writeIndex, finishedWriting, readIndexes);
    
    producerThread.join();

    for (int i = 0; i < numThreads - 1; ++i) {
        parseThreads[i].join();
    }

    for (size_t i = 0; i < numBuffers; ++i) {
        delete[] buffers[i];
    }
    if (!success && writeIndex.load() == 0) {
        // something went potentially really wrong during decompression
        throw std::runtime_error("Errors during decompression");
    }
    return success;
}

template<std::size_t numBuffers>
void XlsxSheet::interleavedFunc(size_t numThreads, const size_t threadId, std::array<unsigned char*, numBuffers>& buffers, const size_t bufferSize, const std::atomic_size_t& writeIndex, const std::atomic_bool& finishedWriting, std::vector<std::atomic_size_t>& readIndexes) {
    constexpr size_t cellValueBufferSize = BUFFER_SIZE; // Max number of characters in cell is 32767
    char cellValueBuffer[cellValueBufferSize];
    size_t cellValueLength = 0;

    size_t offset = 0;

    std::atomic_size_t& readIndex = readIndexes[threadId];
    size_t currentReadBuffer = readIndex.load();

    ElementParser<1> dimension("dimension", {"ref"}, {AttributeType::RANGE});
    ElementParser<0> sheetData("sheetData", {}, {});
    ElementParser<3> c("c", {"r", "t", "s"}, {AttributeType::LOCATION, AttributeType::TYPE, AttributeType::INDEX});
    ElementParser<0> v("v", {}, {});

    // all other threads wait until data structure has been pre-allocated
    if (threadId > 0) {
        while (!mReserved.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            // also abort all other threads if fallback to single
            if (mFallbackSingle.load()) return;
        }
    }

    bool loadNext = false;
    bool continueCell = false;

    while (readIndex.load() < writeIndex.load() || !finishedWriting.load()) {
        if (offset >= bufferSize || buffers[currentReadBuffer % numBuffers][offset] == '\0' || loadNext) {
            const size_t prevBuffer = currentReadBuffer;
            if (c.outside()) {
                // standard leapfrog to next buffer for this thread
                currentReadBuffer = readIndex.load() + numThreads - continueCell;
                continueCell = false;
            } else {
                // cell may not have been fully parsed yet, extend into the next buffer to finish
                currentReadBuffer = readIndex.load() + 1;
                continueCell = true;
            }

            // if a parse extension was long enough (unrealistic) or only 1 thread was specified (more likely)
            // we could end up at the same buffer we are already at
            if (currentReadBuffer != prevBuffer) {
                while (!finishedWriting.load() && currentReadBuffer >= writeIndex.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (finishedWriting.load() && currentReadBuffer > writeIndex.load()) {
                    currentReadBuffer = writeIndex.load();
                }
                if (currentReadBuffer > writeIndex.load() && finishedWriting.load()) break;
                readIndex.store(currentReadBuffer);
                if (buffers[currentReadBuffer % numBuffers][0] == '\0') {
                    continue;
                }
                offset = 0;
            }
            loadNext = false;
        }

        const unsigned char current = buffers[currentReadBuffer % numBuffers][offset];
        ++offset;

        if (!mReserved.load() && !mFallbackSingle.load()) {
            sheetData.process(current);
            dimension.process(current);
            if (dimension.completed() && dimension.hasValue(0)) {
                const auto val = static_cast<const RangeParser&>(dimension.getAttribute(0)).getValue();
                reserve(val.second.first, val.second.second);
                mReserved.store(true);
            } else if (sheetData.inside()) {
                // ! we arrived inside sheet data but without having encountered the dimension tag
                // fallback to single thread parsing
                numThreads = 1;
                mFallbackSingle.store(true);
            }
        }
        bool in_c = c.inside();
        c.process(current);
        if (!in_c && !(continueCell && c.outside())) continue;
        bool in_v = v.inside();
        v.process(current);
        if (!in_v && v.inside()) continue;
        
        if (c.completed()) {
            XlsxColumn::CellType cellType = XlsxColumn::CellType::T_NUMERIC;
            bool dateStyle = false;
            unsigned long rowNumber = 0;
            unsigned long cellColumn = 0;
            if (c.hasValue(0)) {
                const std::pair<unsigned long, unsigned long> val = static_cast<const LocationParser&>(c.getAttribute(0)).getValue();
                cellColumn = val.first;
                rowNumber = val.second;
            }
            if (c.hasValue(1)) cellType = static_cast<const TypeParser&>(c.getAttribute(1)).getValue();
            if (c.hasValue(2)) dateStyle = mParentFile.isDate(static_cast<const IndexParser&>(c.getAttribute(2)).getValue());

            if (cellType == XlsxColumn::CellType::T_STRING_INLINE) {
                //TODO: inline strings are not in the v element, but in t
                cellValueLength = 0;
                cellValueBuffer[0] = 0;
                continue;
            }
            if (cellValueLength == 0 || (static_cast<int>(cellValueLength) < v.getCloseLength())) {
                // no value
                cellValueLength = 0;
                cellValueBuffer[0] = 0;
                continue;
            }

            cellValueBuffer[cellValueLength - v.getCloseLength() + 1] = 0;
            if (rowNumber == 0 || cellColumn == 0 || cellType == XlsxColumn::CellType::T_NONE) {
                throw std::runtime_error("Error when parsing cell");
            }
            //std::cout << "Thread " << threadId << ": " << rowNumber << " / " << cellColumn << ", " << static_cast<int>(cellType) << " (offset " << offset << "): " << cellValueBuffer << std::endl;
            if (cellType == XlsxColumn::CellType::T_NUMERIC) {
                double value = 0;
                const bool isok = fast_double_parser::parse_number(cellValueBuffer, &value);
                if (!isok) {
                    throw std::runtime_error("Error when parsing number");
                }
                if (dateStyle) {
                    placeCell(mParentFile.toDate(value), XlsxColumn::CellType::T_DATE, rowNumber, cellColumn);
                } else {
                    placeCell(value, cellType, rowNumber, cellColumn);
                }
            } else if (cellType == XlsxColumn::CellType::T_STRING) {
                mParentFile.unescape(cellValueBuffer);
                const unsigned long long stringIndex = mParentFile.addDynamicString(cellValueBuffer);
                placeCell(stringIndex, cellType, rowNumber, cellColumn);
            } else {
                const unsigned long long value = extractUnsigned(cellValueBuffer, 0, cellValueLength);
                placeCell(value, cellType, rowNumber, cellColumn);
            }
            cellValueLength = 0;
            cellValueBuffer[0] = 0;
            if (continueCell) loadNext = true;
            continue;
        } else if (continueCell && c.outside()) {
            // extension into next buffer may have not been needed (not actually an open cell)
            loadNext = true;
            continue;
        }
        if (v.inside()) {
            if (cellValueLength >= cellValueBufferSize) {
                throw std::runtime_error("Exceeded cell value buffer size");
            }
            cellValueBuffer[cellValueLength++] = current;
        }
    }
}
