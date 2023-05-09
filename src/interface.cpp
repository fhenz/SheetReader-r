#include <Rcpp.h>
#include <thread>

#include "XlsxFile.h"
#include "XlsxSheet.h"

std::string formatNumber(const double number) {
	char buf[64];
	snprintf(buf, 64, "%lg", number);
	return buf;
}

std::string formatDatetime(const double timestamp) {
	char buf[64];
	time_t t = static_cast<time_t>(std::floor(timestamp));
	struct tm temp = *gmtime(&t); // localtime, not gmtime
	size_t res = ::strftime(buf, 63, "%Y-%m-%d %H:%M:%S", &temp);
	if (res <= 0) {
		return std::string("");
	} else {
		return std::string(buf);
	}
}

void coerceString(const XlsxFile& file, const int ithread, Rcpp::RObject& vector, const size_t index, const XlsxCell& value, const CellType valueType) {
	if (valueType == CellType::T_NUMERIC) {
		static_cast<Rcpp::CharacterVector>(vector)[index] = formatNumber(value.data.real);
	} else if (valueType == CellType::T_STRING_REF) {
		const auto str = file.getString(value.data.integer);
		static_cast<Rcpp::CharacterVector>(vector)[index] = str;
	} else if (valueType == CellType::T_STRING || valueType == CellType::T_STRING_INLINE) {
		const auto& str = file.getDynamicString(ithread, value.data.integer);
		static_cast<Rcpp::CharacterVector>(vector)[index] = Rf_mkCharCE(str.c_str(), CE_UTF8);
	} else if (valueType == CellType::T_BOOLEAN) {
		static_cast<Rcpp::CharacterVector>(vector)[index] = value.data.boolean ? "TRUE" : "FALSE";
	} else if (valueType == CellType::T_DATE) {
		static_cast<Rcpp::CharacterVector>(vector)[index] = formatDatetime(value.data.real);
	}
}

Rcpp::DataFrame cells_to_dataframe(const XlsxFile& file, XlsxSheet& sheet) {
	size_t nColumns = sheet.mDimension.first;
	size_t nRows = sheet.mDimension.second;
	//std::cout << "cells_to_dataframe " << nColumns << " / " << nRows << std::endl;
	if (nRows == 0) {
		// determine max rows (if dimension element not found), columns not needed
		//TODO: this is actually wrong, we need to do it by chunk, not by thread (-> maintain iterator for every thread)
		for (size_t ithread = 0; ithread < sheet.mCells.size(); ++ithread) {
			size_t rowInfos = 0;
			for (auto it = sheet.mLocationInfos[ithread].rbegin(); it != sheet.mLocationInfos[ithread].rend(); ++it) {
				if (it->row == -1ul) {
					rowInfos++;
				} else {
					//std::cout << "determine rows (" << ithread << "): " << it->row << " + " << rowInfos << " > " << nRows << std::endl;
					if (it->row + rowInfos > nRows) nRows = it->row + rowInfos;
					break;
				}
			}
		}
		// at this point nRows is actually the last cell row, which is 0-indexed so +1
		nRows++;
	}
	//std::cout << "rows: " << nRows << " - " << sheet.mHeaders << " - " << sheet.mSkipRows << " => " << (nRows - sheet.mHeaders - sheet.mSkipRows) << std::endl;
	nRows = nRows - sheet.mHeaders - sheet.mSkipRows;
	nColumns = nColumns - sheet.mSkipColumns;
	if (nRows == 0) {
		return Rcpp::DataFrame::create();
	}
	// proxy vector to avoid R garbage collector housekeeping (later turned into Rcpp::List)
	// also allows us to do the conversion without knowing the number of columns beforehand
	std::vector<Rcpp::RObject> proxies;
	// we only reserve and not resize here in case of erroneous <dimension> element (sometimes up to max col/max row even if empty)
	// real size determined during data insertion
	std::vector<std::tuple<XlsxCell, CellType, size_t>> headerCells;
	if (nColumns > 0) {
		proxies.reserve(nColumns);
		headerCells.reserve(nColumns);
	}
	std::vector<CellType> coltypes(nColumns, CellType::T_NONE);
	std::vector<CellType> coerce(nColumns, CellType::T_NONE);

	unsigned long currentColumn = 0;
	long long currentRow = -1;
	std::vector<size_t> currentLocs(sheet.mCells.size(), 0);
	const size_t maxBuffers = sheet.mCells.size() > 0 ? sheet.mCells[0].size() : 0;
	for (size_t buf = 0; buf < maxBuffers; ++buf) {
		for (size_t ithread = 0; ithread < sheet.mCells.size(); ++ithread) {
			if (sheet.mCells[ithread].size() == 0) {
				break;
			}
			//std::cout << buf << ", " << ithread << "/" << sheet.mCells.size() << std::endl;
			const std::vector<XlsxCell> cells = sheet.mCells[ithread].front().first;
			const std::vector<CellType> types = sheet.mCells[ithread].front().second;
			const std::vector<LocationInfo>& locs = sheet.mLocationInfos[ithread];
			size_t& currentLoc = currentLocs[ithread];

			// icell <= cells.size() because there might be location info after last cell
			for (size_t icell = 0; icell <= cells.size(); ++icell) {
				while (currentLoc < locs.size() && locs[currentLoc].buffer == buf && locs[currentLoc].cell == icell) {
					//std::cout << "loc " << currentLoc << "/" << locs.size() << ": " << locs[currentLoc].buffer << " vs " << buf << ", " << locs[currentLoc].cell << " vs " << icell << " (" << locs[currentLoc].column << "/" << locs[currentLoc].row << ")" << std::endl;
					currentColumn = locs[currentLoc].column;
					if (locs[currentLoc].row == -1ul) {
						++currentRow;
					} else {
						currentRow = locs[currentLoc].row;
					}
					++currentLoc;
				}
				if (icell >= cells.size()) break;
				const auto adjustedColumn = currentColumn;
				const auto adjustedRow = currentRow - sheet.mSkipRows;
				const XlsxCell& cell = cells[icell];
				const CellType type = types[icell];

				//std::cout << adjustedColumn << "/" << adjustedRow << std::endl;

				if (adjustedRow >= sheet.mHeaders) {
					// normal (non-header) cell
					if (coltypes.size() <= adjustedColumn) {
						coltypes.resize(adjustedColumn + 1, CellType::T_NONE);
						coerce.resize(adjustedColumn + 1, CellType::T_NONE);
					}
					//std::cout << "coltypes " << adjustedColumn << " / " << coltypes.size() << std::endl;
					if (coltypes[adjustedColumn] == CellType::T_NONE) {
						Rcpp::RObject robj;
						if (type == CellType::T_NUMERIC) {
							robj = Rcpp::NumericVector(nRows, Rcpp::NumericVector::get_na());
						} else if (type == CellType::T_STRING_REF || type == CellType::T_STRING || type == CellType::T_STRING_INLINE) {
							robj = Rcpp::CharacterVector(nRows, Rcpp::CharacterVector::get_na());
						} else if (type == CellType::T_BOOLEAN) {
							robj = Rcpp::LogicalVector(nRows, Rcpp::LogicalVector::get_na());
						} else if (type == CellType::T_DATE) {
							robj = Rcpp::DatetimeVector(nRows, "UTC");
							// fill with NAs, otherwise missing would be 1970-01-01
							for (size_t i = 0; i < nRows; ++i) {
								static_cast<Rcpp::DatetimeVector>(robj)[i] = Rcpp::DatetimeVector::get_na();
							}
						}
						if (type != CellType::T_NONE) {
							if (proxies.size() < adjustedColumn) {
								proxies.reserve(adjustedColumn + 1);
								proxies.resize(adjustedColumn);
								proxies.push_back(robj);
							} else if (proxies.size() > adjustedColumn) {
								proxies[adjustedColumn] = robj;
							} else {
								proxies.push_back(robj);
							}
							coltypes[adjustedColumn] = type;
						}
					}
					if (coltypes[adjustedColumn] != CellType::T_NONE) {
						const CellType col_type = coltypes[adjustedColumn];
						const bool compatible = ((type == col_type)
							|| (type == CellType::T_STRING_REF && col_type == CellType::T_STRING)
							|| (type == CellType::T_STRING_REF && col_type == CellType::T_STRING_INLINE)
							|| (type == CellType::T_STRING && col_type == CellType::T_STRING_REF)
							|| (type == CellType::T_STRING && col_type == CellType::T_STRING_INLINE)
							|| (type == CellType::T_STRING_INLINE && col_type == CellType::T_STRING_REF)
							|| (type == CellType::T_STRING_INLINE && col_type == CellType::T_STRING));
						const unsigned long i = adjustedRow - sheet.mHeaders;
						if (coerce[adjustedColumn] == CellType::T_STRING) {
							Rcpp::RObject& robj = proxies[adjustedColumn];
							coerceString(file, ithread, robj, i, cell, type);
						} else if (compatible) {
							Rcpp::RObject& robj = proxies[adjustedColumn];
							if (type == CellType::T_NUMERIC) {
								static_cast<Rcpp::NumericVector>(robj)[i] = cell.data.real;
							} else if (type == CellType::T_STRING_REF) {
								const auto str = file.getString(cell.data.integer);
								static_cast<Rcpp::CharacterVector>(robj)[i] = str;
							} else if (type == CellType::T_STRING || type == CellType::T_STRING_INLINE) {
								const auto& str = file.getDynamicString(ithread, cell.data.integer);
								static_cast<Rcpp::CharacterVector>(robj)[i] = Rf_mkCharCE(str.c_str(), CE_UTF8);
							} else if (type == CellType::T_BOOLEAN) {
								static_cast<Rcpp::LogicalVector>(robj)[i] = cell.data.boolean;
							} else if (type == CellType::T_DATE) {
								static_cast<Rcpp::DatetimeVector>(robj)[i] = cell.data.real;
							}
						} else if (coerce[adjustedColumn] == CellType::T_NONE) {
							coerce[adjustedColumn] = CellType::T_STRING;
							if (col_type != CellType::T_STRING && col_type != CellType::T_STRING_REF && col_type != CellType::T_STRING_INLINE) {
								// convert existing
								Rcpp::RObject& robj = proxies[adjustedColumn];
								Rcpp::RObject newObj = Rcpp::CharacterVector(nRows, Rcpp::CharacterVector::get_na());
								for (size_t i = 0; i < nRows; ++i) {
									if (col_type == CellType::T_NUMERIC) {
										if (Rcpp::NumericVector::is_na(static_cast<Rcpp::NumericVector>(robj)[i])) continue;
										static_cast<Rcpp::CharacterVector>(newObj)[i] = formatNumber(static_cast<Rcpp::NumericVector>(robj)[i]);
									} else if (col_type == CellType::T_BOOLEAN) {
										if (Rcpp::LogicalVector::is_na(static_cast<Rcpp::LogicalVector>(robj)[i])) continue;
										static_cast<Rcpp::CharacterVector>(newObj)[i] = static_cast<Rcpp::LogicalVector>(robj)[i] ? "TRUE" : "FALSE";
									} else if (col_type == CellType::T_DATE) {
										if (Rcpp::DatetimeVector::is_na(static_cast<Rcpp::DatetimeVector>(robj)[i])) continue;
										static_cast<Rcpp::CharacterVector>(newObj)[i] = formatDatetime(static_cast<Rcpp::DatetimeVector>(robj)[i]);
									}
								}
								proxies[adjustedColumn] = newObj;
							}
							coerceString(file, ithread, proxies[adjustedColumn], i, cell, type);
						}
					}
				} else {
					// header cell
					if (headerCells.size() <= adjustedColumn) {
						headerCells.resize(adjustedColumn + 1);
					}
					headerCells[adjustedColumn] = std::make_tuple(cell, type, ithread);
				}
				++currentColumn;
			}
			sheet.mCells[ithread].pop_front();
		}
	}
	//std::cout << "To dataframe " << proxies.size() << ", " << headerCells.size() << std::endl;
	size_t numCols = std::max(proxies.size(), headerCells.size());
	Rcpp::List lst(numCols);
	Rcpp::CharacterVector names(numCols);
	for (size_t i = 0; i < numCols; ++i) {
		// data
		if (i >= proxies.size() || coltypes[i] == CellType::T_NONE || proxies[i] == R_NilValue) {
			lst[i] = Rcpp::NumericVector(nRows, Rcpp::NumericVector::get_na());
		} else {
			lst[i] = proxies[i];
		}
		// header
		if (i < headerCells.size() && std::get<1>(headerCells[i]) != CellType::T_NONE) {
			auto& cell = std::get<0>(headerCells[i]);
			auto& type = std::get<1>(headerCells[i]);
			if (type == CellType::T_NUMERIC) {
				names[i] = cell.data.real;
			} else if (type == CellType::T_STRING_REF) {
				names[i] = file.getString(cell.data.integer);
			} else if (type == CellType::T_STRING || type == CellType::T_STRING_INLINE) {
				const auto& str = file.getDynamicString(std::get<2>(headerCells[i]), cell.data.integer);
				names[i] = Rf_mkCharCE(str.c_str(), CE_UTF8);
			} else if (type == CellType::T_BOOLEAN) {
				names[i] = cell.data.boolean;
			} else if (type == CellType::T_DATE) {
				names[i] = cell.data.real;
			}
		} else {
			names[i] = "Column" + std::to_string(i);
		}
	}
	Rcpp::DataFrame result(lst);
	result.attr("names") = names;
	return result;
}

// [[Rcpp::export]]
Rcpp::DataFrame read_xlsx(const std::string path, SEXP sheet = R_NilValue, bool headers = true, int skip_rows = 0, int skip_columns = 0, int num_threads = -1) {
	// manually convert 'sheet' (instead of by Rcpp) to allow string & number input
	std::string sheetName;
	int sheetNumber = 0;
	int type = TYPEOF(sheet);
	if (Rf_length(sheet) > 1) {
		Rcpp::stop("'sheet' must be a single string or positive number");
	}
	if (type == NILSXP) {
		sheetNumber = 1;
	} else if (type == STRSXP) {
		sheetName = Rcpp::as<std::string>(sheet);
	} else if (type == INTSXP || type == REALSXP) {
		sheetNumber = Rcpp::as<int>(sheet);
		if (sheetNumber < 1) Rcpp::stop("'sheet' must be a single string or positive number");
	} else {
		Rcpp::stop("'sheet' must be a single string or positive number");
	}
	if (skip_rows < 0) skip_rows = 0;
	if (skip_columns < 0) skip_columns = 0;

	bool parallel = true;
	if (num_threads == -1) {
		// automatically decide number of threads
		num_threads = std::thread::hardware_concurrency();
		if (num_threads <= 0) {
			num_threads = 1;
		}
		// limit impact on user machine
		if (num_threads > 6 && num_threads <= 10) num_threads = 6;
		// really diminishing returns with higher number of threads
		if (num_threads > 10) num_threads = 10;
	}
	if (num_threads <= 1) {
		num_threads = 1;
		parallel = false;
	}

	try {
		XlsxFile file(path);
		file.mParallelStrings = parallel;
		file.parseSharedStrings();

		XlsxSheet fsheet = sheetNumber > 0 ? file.getSheet(sheetNumber) : file.getSheet(sheetName);
		fsheet.mHeaders = headers;
		// if parallel we need threads for string parsing
		// for interleaved, both sheet & strings need additional thread for decompression (meaning min is 2)
		int act_num_threads = num_threads - parallel * 2 - (num_threads > 1);
		if (act_num_threads <= 0) act_num_threads = 1;
		bool success = fsheet.interleaved(skip_rows, skip_columns, act_num_threads);
		file.finalize();
		if (!success) {
			Rcpp::warning("There were errors while reading the file, please check output for consistency.");
		}

		return cells_to_dataframe(file, fsheet);
	} catch (const std::exception& e) {
		Rcpp::stop("Failed to read file: " + std::string(e.what()));
	}
	return R_NilValue;
}
