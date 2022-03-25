\name{read_xlsx}
\alias{read_xlsx}
\title{
Parse data from a xlsx file
}
\description{
Parse tabular data from a sheet inside a xlsx file into a data.frame
}
\usage{
read_xlsx(
    path,
    sheet = NULL,
    headers = TRUE,
    skip_rows = 0,
    skip_columns = 0,
    method = "efficient",
    num_threads = -1
)
}
\arguments{
\item{path}{The path to the xlsx file that is to be parsed.}

\item{sheet}{Which sheet in the file to parse. Can be either the index/position (1 = first sheet) or name.
    By default parses the first sheet.
}

\item{headers}{Whether to interpret the first row as column names.}

\item{skip_rows}{How many rows should be skipped before values are read.}

\item{skip_columns}{How many columns should be skipped before values are read.}

\item{method}{What method should be used to parse the file.
    Can be either "efficient" or "fast". See 'Details' for more information.
}

\item{num_threads}{The number of threads to use for parsing.
    Will be automatically determined if not provided.
    Note that the "efficient" method uses a minimum of 2 threads even if only 1 is specified.
}
}
\value{
    data.frame
}
\details{
    2 methods for parsing are provided; "efficient" and "fast".

    The "efficient" method achieves minimal memory usage by only ever decompressing a small part
    of the file. This part is parsed and then replaced with the next decompressed content.
    Decompression and parsing are done by separate threads (parsing is additionally divided among multiple threads)
    to achieve good runtimes.
    Unfortunately, currently using multiple parse threads relies on cells also storing their position in the sheet
    within themselves. This is done by Excel itself, but for other xlsx-generating tools this may not be guaranteed.
    In these cases only 1 parse thread can be used.

    The "fast" method decompresses the whole file into memory using the full-buffer optimized 'libdeflate' library.
    This is faster than the "efficient" method but obviously requires more memory.
    If you are sure you have enough memory you can specify this method to get slightly better runtime 
    (be aware that the decompressed content can be significantly larger than the shown file size).
}
\examples{
exampleFile <- system.file("extdata", "multi-test.xlsx", package = "SheetReader")

# Read first sheet of the file, using first row as column names
df1 <- read_xlsx(exampleFile, sheet = 1, headers = TRUE)
head(df1)

# Read the "encoding" sheet, skipping 1 row and not using the next row as column names
df2 <- read_xlsx(exampleFile, sheet = "encoding", headers = FALSE, skip_rows = 1)
head(df2)
}
