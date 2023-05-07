#include <Rcpp.h>
#include <thread>

#include "XlsxFile.h"
#include "XlsxSheet.h"
#include "XlsxColumn.h"

Rcpp::RObject column_to_list(const XlsxFile& file, const XlsxColumn& column, const std::size_t size) {
	size_t c = 0;
	if (column.mCells.size() == 0) return Rcpp::NumericVector(size, Rcpp::NumericVector::get_na());
	XlsxColumn::CellType col_type = column.mTypes[c++];
	while (col_type == XlsxColumn::CellType::T_NONE && c < column.mTypes.size()) col_type = column.mTypes[c++];
    Rcpp::RObject robj;
    if (col_type == XlsxColumn::CellType::T_NUMERIC) {
        robj = Rcpp::NumericVector(size, Rcpp::NumericVector::get_na());
    } else if (col_type == XlsxColumn::CellType::T_STRING_REF || col_type == XlsxColumn::CellType::T_STRING) {
        robj = Rcpp::CharacterVector(size, Rcpp::CharacterVector::get_na());
    } else if (col_type == XlsxColumn::CellType::T_BOOLEAN) {
        robj = Rcpp::LogicalVector(size, Rcpp::LogicalVector::get_na());
    } else if (col_type == XlsxColumn::CellType::T_DATE) {
		robj = Rcpp::DatetimeVector(size, "UTC");
		// fill with NAs, otherwise missing would be 1970-01-01
		for (size_t i = 0; i < size; ++i) {
			static_cast<Rcpp::DatetimeVector>(robj)[i] = Rcpp::DatetimeVector::get_na();
		}
    } else {
		// return empty vector (filled with NAs)
		return Rcpp::NumericVector(size, Rcpp::NumericVector::get_na());
    }

    for (unsigned long i = 0; i < column.mCells.size(); ++i) {
        const XlsxColumn::CellType cell_type = column.mTypes[i];
		bool compatible = ((cell_type == col_type)
			|| (cell_type == XlsxColumn::CellType::T_STRING_REF && col_type == XlsxColumn::CellType::T_STRING)
			|| (cell_type == XlsxColumn::CellType::T_STRING && col_type == XlsxColumn::CellType::T_STRING_REF));
        if (!compatible) continue;
        if (col_type == XlsxColumn::CellType::T_NUMERIC) {
            static_cast<Rcpp::NumericVector>(robj)[i] = column.mCells[i].real;
        } else if (cell_type == XlsxColumn::CellType::T_STRING_REF) {
            const auto str = column.mParentSheet.mParentFile.getString(column.mCells[i].integer);
            static_cast<Rcpp::CharacterVector>(robj)[i] = str;
        } else if (cell_type == XlsxColumn::CellType::T_STRING) {
            const auto& str = column.mParentSheet.mParentFile.getDynamicString(column.mCells[i].integer);
            static_cast<Rcpp::CharacterVector>(robj)[i] = Rf_mkCharCE(str.c_str(), CE_UTF8);
        } else if (cell_type == XlsxColumn::CellType::T_BOOLEAN) {
            static_cast<Rcpp::LogicalVector>(robj)[i] = column.mCells[i].boolean;
        } else if (cell_type == XlsxColumn::CellType::T_DATE) {
            static_cast<Rcpp::DatetimeVector>(robj)[i] = column.mCells[i].real;
        } else {
			// currently, if the cell type mismatches the column type, the value remains NA
        }
    }
    return robj;
}

Rcpp::DataFrame sheet_to_dataframe(const XlsxFile& file, XlsxSheet& sheet) {
	Rcpp::List lst = Rcpp::List(sheet.mColumns.size());
	Rcpp::CharacterVector names(sheet.mColumns.size());
	size_t max_size = 0;
	for (const auto& column : sheet.mColumns) if (max_size < column.mCells.size()) max_size = column.mCells.size();
	int ci = 0;
	for (auto& column : sheet.mColumns) {
		if (column.mHasHeader) {
			if (column.mHeaderType == XlsxColumn::CellType::T_STRING_REF) {
				names[ci] = sheet.mParentFile.getString(column.mHeader.integer);
			} else if (column.mHeaderType == XlsxColumn::CellType::T_STRING) {
				names[ci] = Rf_mkCharCE(sheet.mParentFile.getDynamicString(column.mHeader.integer).c_str(), CE_UTF8);
			} else {
				//TODO: other types? e.g. convert number to string
				names[ci] = "Column" + std::to_string(ci);
			}
		} else {
			names[ci] = "Column" + std::to_string(ci);
		}
		lst[ci] = column_to_list(file, column, max_size);
		column.clear();
		ci++;
	}
	Rcpp::DataFrame result(lst);
	result.attr("names") = names;
	return result;
}

Rcpp::DataFrame cells_to_dataframe(const XlsxFile& file, XlsxSheet& sheet) {
	const size_t nColumns = sheet.mDimension.first;
	size_t nRows = sheet.mDimension.second;
	/*for (size_t ithread = 0; ithread < sheet.mLocationInfos.size(); ++ithread) {
		std::cout << "LocInfo: " << sheet.mLocationInfos[ithread].size() << " for " << sheet.mCells[ithread].size() << std::endl;
	}*/
	if (nRows == 0) {
		// determine max rows (if dimension element not found), columns not needed
		//TODO: this is actually wrong, we need to do it by chunk, not by thread (-> maintain iterator for every thread)
		for (size_t ithread = 0; ithread < sheet.mCells.size(); ++ithread) {
			size_t rowInfos = 0;
			for (auto it = sheet.mLocationInfos[ithread].rbegin(); it != sheet.mLocationInfos[ithread].rend(); ++it) {
				if (it->row == -1) {
					rowInfos++;
				} else {
					if (it->row + rowInfos > nRows) nRows = nRows = it->row + rowInfos;
					break;
				}
			}
		}
	}
	// proxy vector to avoid R garbage collector housekeeping (later turned into Rcpp::List)
	// also allows us to do the conversion without knowing the number of columns beforehand
	std::vector<Rcpp::RObject> proxies;
	proxies.reserve(nColumns);
	Rcpp::CharacterVector names(nColumns);
	std::vector<XlsxColumn::CellType> coltypes(nColumns, XlsxColumn::CellType::T_NONE);
	int ci = 0;
	for (auto& column : sheet.mColumns) {
		names[ci] = "Column" + std::to_string(ci);
		ci++;
	}

	unsigned long currentColumn = 0;
	long long currentRow = -1;
	std::vector<size_t> currentLocs(sheet.mCells.size(), 0);
	const size_t maxBuffers = sheet.mCells.size() > 0 ? sheet.mCells[0].size() : 0;
	for (size_t buf = 0; buf < maxBuffers; ++buf) {
		for (size_t ithread = 0; ithread < sheet.mCells.size(); ++ithread) {
			if (sheet.mCells[ithread].size() == 0) {
				break;
			}
			const std::vector<XlsxCell> cells = sheet.mCells[ithread].front().first;
			const std::vector<XlsxColumn::CellType> types = sheet.mCells[ithread].front().second;
			const std::vector<LocationInfo>& locs = sheet.mLocationInfos[ithread];
			size_t& currentLoc = currentLocs[ithread];
			/*size_t icell = 0;
			while (icell < cells.size()) {
				const size_t to = (currentLoc < locs.size() && locs[currentLoc].buffer == buf) ? locs[currentLoc].cell : cells.size();
				for (; icell < to; ++icell) {
					const XlsxCell& cell = cells[icell];
					if (currentRow > 0) {
						if (coltypes[currentColumn] == XlsxColumn::CellType::T_NONE) {
							Rcpp::RObject robj;
							if (cell.type == XlsxColumn::CellType::T_NUMERIC) {
								robj = Rcpp::NumericVector(nRows, Rcpp::NumericVector::get_na());
							} else if (cell.type == XlsxColumn::CellType::T_STRING_REF || cell.type == XlsxColumn::CellType::T_STRING) {
								robj = Rcpp::CharacterVector(nRows, Rcpp::CharacterVector::get_na());
							} else if (cell.type == XlsxColumn::CellType::T_BOOLEAN) {
								robj = Rcpp::LogicalVector(nRows, Rcpp::LogicalVector::get_na());
							} else if (cell.type == XlsxColumn::CellType::T_DATE) {
								robj = Rcpp::DatetimeVector(nRows, "UTC");
								// fill with NAs, otherwise missing would be 1970-01-01
								for (size_t i = 0; i < nRows; ++i) {
									static_cast<Rcpp::DatetimeVector>(robj)[i] = Rcpp::DatetimeVector::get_na();
								}
							}
							if (cell.type != XlsxColumn::CellType::T_NONE) {
								lst[currentColumn] = robj;
								proxies[currentColumn] = robj;
								coltypes[currentColumn] = cell.type;
							}
						}
						if (coltypes[currentColumn] != XlsxColumn::CellType::T_NONE) {
							const XlsxColumn::CellType col_type = coltypes[currentColumn];
							const bool compatible = ((cell.type == col_type)
								|| (cell.type == XlsxColumn::CellType::T_STRING_REF && col_type == XlsxColumn::CellType::T_STRING)
								|| (cell.type == XlsxColumn::CellType::T_STRING && col_type == XlsxColumn::CellType::T_STRING_REF));
							if (compatible) {
								//Rcpp::RObject robj = lst[currentColumn];
								Rcpp::RObject& robj = proxies[currentColumn];
								const unsigned long i = currentRow - 1;
								//std::cout << "Insert " << currentColumn << "/" << i << ": " << static_cast<int>(cell.type) << " vs " << static_cast<int>(coltypes[currentColumn]) << std::endl;
								if (cell.type == XlsxColumn::CellType::T_NUMERIC) {
									static_cast<Rcpp::NumericVector>(robj)[i] = cell.data.real;
								} else if (cell.type == XlsxColumn::CellType::T_STRING_REF) {
									const auto str = file.getString(cell.data.integer);
									static_cast<Rcpp::CharacterVector>(robj)[i] = str;
								} else if (cell.type == XlsxColumn::CellType::T_STRING) {
									const auto& str = file.getDynamicString(cell.data.integer);
									static_cast<Rcpp::CharacterVector>(robj)[i] = Rf_mkCharCE(str.c_str(), CE_UTF8);
								} else if (cell.type == XlsxColumn::CellType::T_BOOLEAN) {
									static_cast<Rcpp::LogicalVector>(robj)[i] = cell.data.boolean;
								} else if (cell.type == XlsxColumn::CellType::T_DATE) {
									static_cast<Rcpp::DatetimeVector>(robj)[i] = cell.data.real;
								}
							}
						}
					}
					++currentColumn;
				}
				while (locs[currentLoc].buffer == buf && locs[currentLoc].cell == icell) {
					currentColumn = locs[currentLoc].column;
					if (locs[currentLoc].row == -1) {
						++currentRow;
					} else {
						currentRow = locs[currentLoc].row;
					}
					++currentLoc;
				}
			}*/
			// icell <= cells.size() because there might be location info after last cell
			for (size_t icell = 0; icell <= cells.size(); ++icell) {
				while (currentLoc < locs.size() && locs[currentLoc].buffer == buf && locs[currentLoc].cell == icell) {
					currentColumn = locs[currentLoc].column;
					if (locs[currentLoc].row == -1) {
						//std::cout << "LocInfo row " << locs[currentLoc].column << "/" << locs[currentLoc].row << " in " << ithread << ", " << buf << ", " << icell << ", " << currentLoc << std::endl;
						++currentRow;
					} else {
						currentRow = locs[currentLoc].row;
					}
					++currentLoc;
				}
				if (icell >= cells.size()) break;
				const XlsxCell& cell = cells[icell];
				const XlsxColumn::CellType type = types[icell];

				//if (currentColumn > maxCol) maxCol = currentColumn;

				if (currentRow > 0) {
					if (coltypes[currentColumn] == XlsxColumn::CellType::T_NONE) {
						Rcpp::RObject robj;
						if (type == XlsxColumn::CellType::T_NUMERIC) {
							robj = Rcpp::NumericVector(nRows, Rcpp::NumericVector::get_na());
						} else if (type == XlsxColumn::CellType::T_STRING_REF || type == XlsxColumn::CellType::T_STRING) {
							robj = Rcpp::CharacterVector(nRows, Rcpp::CharacterVector::get_na());
						} else if (type == XlsxColumn::CellType::T_BOOLEAN) {
							robj = Rcpp::LogicalVector(nRows, Rcpp::LogicalVector::get_na());
						} else if (type == XlsxColumn::CellType::T_DATE) {
							robj = Rcpp::DatetimeVector(nRows, "UTC");
							// fill with NAs, otherwise missing would be 1970-01-01
							for (size_t i = 0; i < nRows; ++i) {
								static_cast<Rcpp::DatetimeVector>(robj)[i] = Rcpp::DatetimeVector::get_na();
							}
						}
						if (type != XlsxColumn::CellType::T_NONE) {
							//lst[currentColumn] = robj;
							//proxies[currentColumn] = robj;
							if (proxies.size() < currentColumn) {
								proxies.reserve(currentColumn + 1);
								proxies.resize(currentColumn);
								proxies.push_back(robj);
							} else if (proxies.size() > currentColumn) {
								proxies[currentColumn] = robj;
							} else {
								proxies.push_back(robj);
							}
							coltypes[currentColumn] = type;
						}
					}
					if (coltypes[currentColumn] != XlsxColumn::CellType::T_NONE) {
						const XlsxColumn::CellType col_type = coltypes[currentColumn];
						const bool compatible = ((type == col_type)
							|| (type == XlsxColumn::CellType::T_STRING_REF && col_type == XlsxColumn::CellType::T_STRING)
							|| (type == XlsxColumn::CellType::T_STRING && col_type == XlsxColumn::CellType::T_STRING_REF));
						if (compatible) {
							Rcpp::RObject& robj = proxies[currentColumn];
							const unsigned long i = currentRow - 1;
							//std::cout << "Insert " << currentColumn << "/" << i << ": " << static_cast<int>(cell.type) << " vs " << static_cast<int>(coltypes[currentColumn]) << std::endl;
							if (type == XlsxColumn::CellType::T_NUMERIC) {
								static_cast<Rcpp::NumericVector>(robj)[i] = cell.data.real;
							} else if (type == XlsxColumn::CellType::T_STRING_REF) {
								const auto str = file.getString(cell.data.integer);
								static_cast<Rcpp::CharacterVector>(robj)[i] = str;
							} else if (type == XlsxColumn::CellType::T_STRING) {
								const auto& str = file.getDynamicString(cell.data.integer);
								static_cast<Rcpp::CharacterVector>(robj)[i] = Rf_mkCharCE(str.c_str(), CE_UTF8);
							} else if (type == XlsxColumn::CellType::T_BOOLEAN) {
								static_cast<Rcpp::LogicalVector>(robj)[i] = cell.data.boolean;
							} else if (type == XlsxColumn::CellType::T_DATE) {
								static_cast<Rcpp::DatetimeVector>(robj)[i] = cell.data.real;
							}
						}
					}
				}
				++currentColumn;
			}
			sheet.mCells[ithread].pop_front();
		}
	}
	Rcpp::List lst(proxies.size());
	for (size_t i = 0; i < proxies.size(); ++i) {
		if (coltypes[i] == XlsxColumn::CellType::T_NONE) {
			lst[i] = Rcpp::NumericVector(nRows, Rcpp::NumericVector::get_na());
		} else {
			lst[i] = proxies[i];
		}
	}
	Rcpp::DataFrame result(lst);
	result.attr("names") = names;
	return result;
}

// [[Rcpp::export]]
Rcpp::DataFrame read_xlsx(const std::string path, SEXP sheet = R_NilValue, bool headers = true, int skip_rows = 0, int skip_columns = 0, const std::string method = "efficient", int num_threads = -1) {
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

	if (method != "efficient" && method != "fast") {
		Rcpp::stop("'method' must be either 'efficient' or 'fast'");
	}

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


	XlsxFile file(path);
	file.mParallelStrings = parallel;
	file.mStringsConsecutive = (method == "fast");
	file.parseSharedStrings();

	XlsxSheet fsheet = sheetNumber > 0 ? file.getSheet(sheetNumber) : file.getSheet(sheetName);
	fsheet.mHeaders = headers;
	// if parallel we need threads for string parsing
	bool success = false;
	if (method == "efficient") {
		// if "efficient", both sheet & strings need additional thread for decompression (meaning min is 2)
		int act_num_threads = num_threads - parallel * 2 - (num_threads > 1);
		if (act_num_threads <= 0) act_num_threads = 1;
		success = fsheet.interleaved(skip_rows, skip_columns, act_num_threads);
	} else {
		int act_num_threads = num_threads - parallel;
		if (act_num_threads <= 0) act_num_threads = 1;
		success = fsheet.consecutive(skip_rows, skip_columns, act_num_threads);
	}
	file.finalize();
	if (!success) {
		Rcpp::warning("There were errors while reading the file, please check output for consistency.");
	}

	return method == "efficient" ? cells_to_dataframe(file, fsheet) : sheet_to_dataframe(file, fsheet);
	//return sheet_to_dataframe(file, fsheet);
}
