# SheetReader
![](https://www.r-pkg.org/badges/version/SheetReader)
![](https://cranlogs.r-pkg.org/badges/grand-total/SheetReader?color=brightgreen)

SheetReader provides functionality to read tabular data from Excel OOXML (.xlsx) files.
This repository integrates SheetReader into a [R](https://www.R-project.org/) package, with [`Rcpp`](https://CRAN.R-project.org/package=Rcpp) serving as the interface for the parsing code written in C++.

SheetReader is available via [CRAN](https://cran.r-project.org/package=SheetReader):
```r
install.packages("SheetReader")
```

## Overview
SheetReader uses incremental decompression with buffer recycling to keep memory usage minimal, while employing multithreading where possible to speed up the parsing process.

While there are existing packages for parsing Excel files into R (notably [`readxl`](https://github.com/tidyverse/readxl) and [`openxlsx`](https://github.com/ycphs/openxlsx)), I was not satisfied with their performance when processing large files.
`readxl` uses the `RapidXML` library to completely parse the Excel documents into full XML/DOM trees, which can get prohibitively expensive (dozens of gigabytes for files upwards of 200,000 rows by 100 columns).
`openxlsx` is more efficient in terms of memory usage but generally slower than `readxl`.  
Benchmarks on files of roughly these sizes (200,000 rows by 100 columns) indicate `SheetReader` to be about 3x faster with 20x less memory usage than `readxl`, and 15x faster with 10x less memory than `openxlsx`.
Be aware that these are relatively old benchmarking results (early 2021) and obviously depend on the file being parsed (`SheetReader` takes advantage of some optional, but usually present, features of the OOXML format to improve performance).  
Having said that, this package is very bare-bones and if you require anything other than parsing tabular data (e.g. retrieving sheet names, writing Excel files) then you should have a look at the other mentioned packages.  
Additionally, the transformation to R dataframe currently assumes homogenous columns.
If cell types in a column don't match the first non-blank cell, they are returned as NA.

## SheetReader Paper
SheetReader was presented at [DOLAP 2022](https://sites.google.com/view/dolap2022/) (colocated with [EDBT](https://conferences.inf.ed.ac.uk/edbticdt2022/)). The paper is open access and can be found [here](http://ceur-ws.org/Vol-3130/paper5.pdf).
If you use SheetReader in your academic project, please cite it with the following bibtex entry:
```
@inproceedings{DBLP:conf/dolap/HenzeGZM22,
  author    = {Felix Henze and
               Haralampos Gavriilidis and
               Eleni Tzirita Zacharatou and
               Volker Markl},
  editor    = {Kostas Stefanidis and
               Lukasz Golab},
  title     = {Efficient Specialized Spreadsheet Parsing for Data Science},
  booktitle = {Proceedings of the 24th International Workshop on Design, Optimization,
               Languages and Analytical Processing of Big Data {(DOLAP)} co-located
               with the 25th International Conference on Extending Database Technology
               and the 25th International Conference on Database Theory {(EDBT/ICDT}
               2022), Edinburgh, UK, March 29, 2022},
  series    = {{CEUR} Workshop Proceedings},
  volume    = {3130},
  pages     = {41--50},
  publisher = {CEUR-WS.org},
  year      = {2022},
  url       = {http://ceur-ws.org/Vol-3130/paper5.pdf},
  timestamp = {Thu, 23 Jun 2022 19:58:25 +0200},
  biburl    = {https://dblp.org/rec/conf/dolap/HenzeGZM22.bib},
  bibsource = {dblp computer science bibliography, https://dblp.org}
}
```

## Acknowledgements
SheetReader includes and uses the following C/C++ libraries:  
- [miniz](https://github.com/richgel999/miniz) for ZIP archive operations and decompression
- [libdeflate](https://github.com/ebiggers/libdeflate) for optimized full-buffer decompression
- [fast_double_parser](https://github.com/lemire/fast_double_parser) for optimized number parsing
