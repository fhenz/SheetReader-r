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

\item{num_threads}{The number of threads to use for parsing.
    Will be automatically determined if not provided.
}
}
\value{
    data.frame
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
