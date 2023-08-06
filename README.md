# SheetReader (Pure C++)
Compile with
```
g++ src/PureCpp.cpp src/XlsxFile.cpp src/XlsxSheet.cpp src/miniz/miniz.cpp -o out
```
or corresponding build-system equivalent.

Currently hard-coded to read in the `escape` sheet of the included `multi-test.xlsx` file.
