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
	if (num_threads <= 1) parallel = false;

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
		success = fsheet.interleaved(skip_rows, skip_columns, num_threads - parallel * 2 - (num_threads > 1));
	} else {
		success = fsheet.consecutive(skip_rows, skip_columns, num_threads - parallel);
	}
	file.finalize();
	if (!success) {
		Rcpp::warning("There were errors while reading the file, please check output for consistency.");
	}

	return sheet_to_dataframe(file, fsheet);
}
