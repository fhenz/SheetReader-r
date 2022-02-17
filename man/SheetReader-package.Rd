\name{SheetReader-package}
\alias{SheetReader-package}
\alias{SheetReader}
\docType{package}
\title{
  Fast and efficient xlsx parsing
}
\description{
  Uses C++ via the 'Rcpp' package to parse modern Excel files ('.xlsx').
  Memory usage is kept minimal by decompressing only parts of the file at a time,
  while employing multiple threads to achieve significant runtime reduction.
}
\details{
  The only function provided by this package is \code{\link[=read_xlsx]{read_xlsx()}},
  with options to determine parsing behaviour.
}
\author{
Felix Henze

Maintainer: Felix Henze <felixhenze0@gmail.com>
}
\keyword{ package }
