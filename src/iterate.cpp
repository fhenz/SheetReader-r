#include <thread>
#include <iostream>

#include "XlsxFile.h"
#include "XlsxSheet.h"

// not for R interface
void iterate(const std::string path, const std::string sheet = "", bool headers = true, int skip_rows = 0, int skip_columns = 0, int num_threads = -1) {
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

		XlsxSheet fsheet = sheet == "" ? file.getSheet(1) : file.getSheet(sheet);
		fsheet.mHeaders = headers;
		// if parallel we need threads for string parsing
		// for interleaved, both sheet & strings need additional thread for decompression (meaning min is 2)
		int act_num_threads = num_threads - parallel * 2 - (num_threads > 1);
		if (act_num_threads <= 0) act_num_threads = 1;
		bool success = fsheet.interleaved(skip_rows, skip_columns, act_num_threads);
		file.finalize();
		if (!success) {
			std::cout << "Warning: There were errors while reading the file, please check output for consistency." << std::endl;
		}

		std::cout << "Columns: " << fsheet.mDimension.first << " / Rows: " << fsheet.mDimension.second << std::endl;
		// get sheet rows
		while (true) {
			// pair contains (row number, cells)
			const std::pair<size_t, std::vector<XlsxCell>> row = fsheet.nextRow();
			// cell vector is empty if no rows remaining
			if (row.second.size() == 0) break;
			if (row.first == 0 && headers) {
				//TODO: first row & headers flag
				continue;
			}
			const std::vector<XlsxCell>& cells = row.second;
			for (size_t i = 0; i < cells.size(); ++i) {
				// cell has .type (CellType) and .data (union of double, unsigned long long, bool)
				const XlsxCell& cell = cells[i];
				
				if (cell.type == CellType::T_NUMERIC) {
					// simple numeric value, could be integer or double (Excel differentiates by style)
					//const double value = cell.data.real;
				} else if (cell.type == CellType::T_STRING_REF) {
					// string value (from the global string table)
					//const auto value = file.getString(cell.data.integer);
				} else if (cell.type == CellType::T_STRING || cell.type == CellType::T_STRING_INLINE) {
					// string value (specified inline)
					//const std::string& value = file.getDynamicString(-1, cell.data.integer);
				} else if (cell.type == CellType::T_BOOLEAN) {
					// boolean value
					//const bool value = cell.data.boolean;
				} else if (cell.type == CellType::T_DATE) {
					// datetime value, already as unix timestamp (seconds since 1970), Excel stores as number of days since 1900
					//const double value = cell.data.real;
					//const std::string value = formatDatetime(cell.data.real);
				} else {
					//NULL (T_NONE)
				}
			}
		}
	} catch (const std::exception& e) {
		std::cout << "Failed to read file: " << e.what() << std::endl;
	}
}
