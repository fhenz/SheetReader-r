# SheetReader

SheetReader provides functionality to read tabular data from Excel OOXML (.xlsx) files.
This repository integrates SheetReader into a [R](https://www.R-project.org/) package, with [`Rcpp`](https://CRAN.R-project.org/package=Rcpp) serving as the interface for the parsing code written in C++.

SheetReader uses incremental decompression with buffer recycling to keep memory usage minimal, while employing multithreading where possible to speed up the parsing process.

While there are existing packages for parsing Excel files into R (notably [`readxl`](https://github.com/tidyverse/readxl) and [`openxlsx`](https://github.com/ycphs/openxlsx)), I was not satisfied with their performance when processing large files.
`readxl` uses the `RapidXML` library to completely parse the Excel documents into full XML/DOM trees, which can get prohibitively expensive (dozens of gigabytes for files upwards of 200,000 rows by 100 columns).
`openxlsx` is more efficient in terms of memory usage but generally slower than `readxl`.  
Benchmarks on files of roughly these sizes (200,000 rows by 100 columns) indicate `SheetReader` to be about 3x faster with 20x less memory usage than `readxl`, and 15x faster with 10x less memory than `openxlsx`.
Be aware that these are relatively old benchmarking results (early 2021) and obviously depend on the file being parsed (`SheetReader` takes advantage of some optional, but usually present, features of the OOXML format to improve performance).  
Having said that, this package is very bare-bones and if you require anything other than parsing tabular data (e.g. retrieving sheet names, writing Excel files) then you should have a look at the other mentioned packages.

SheetReader includes and uses the following C/C++ libraries:  
- [miniz](https://github.com/richgel999/miniz) for ZIP archive operations and decompression
- [libdeflate](https://github.com/ebiggers/libdeflate) for optimized full-buffer decompression
- [fast_double_parser](https://github.com/lemire/fast_double_parser) for optimized number parsing
