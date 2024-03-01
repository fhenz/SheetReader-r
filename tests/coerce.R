library(SheetReader)
options(stringsAsFactors = FALSE)
data <- read_xlsx(system.file("extdata", "multi-test.xlsx", package = "SheetReader"), sheet="coerce", col_types=c("text", "numeric", "numeric", "date", "date", "logical", "numeric", "numeric"))
base_date <- as.POSIXct("1900-01-01", "UTC")
date1 <- as.POSIXct("2021-01-01", "UTC")
date2 <- as.POSIXct("2020-04-01 13:37", "UTC")
compare <- function(x, y) {
	(length(x) == length(y)) && all((x == y) | (if(all(is.numeric(x))) (abs(x - y) < 0.0001) else FALSE) | (is.na(x) & is.na(y)))
}
stopifnot(colnames(data) == c("Text", "Integer", "Real", "Date", "DateTime", "Boolean", "Formula", "Error", "Blank"))
stopifnot(compare(data[, "Text"], c("Blabla", NA, "#REF!", "14", "1", "43922.567361111112", "44197", "-234.72389999999999", "29384723")))
stopifnot(compare(data[, "Integer"], c(29384723, NA, NA, NA, 14, 1, 43922.5674, 44197, -234.7239)))
stopifnot(compare(data[, "Real"], c(-234.7239, 29384723, NA, NA, NA, 14, 1, 43922.5674, 44197)))
# for dates here: 1 == base_date so we have to subtract 1; then due to excel stuff if <61 add +1
stopifnot(compare(data[, "Date"], c(date1, base_date + ((-234.7239 - 1) * 86400), base_date + ((29384723 - 2) * 86400), NA, NA, NA, base_date + ((14 - 1) * 86400), base_date + ((1 - 1) * 86400), date2)))
stopifnot(compare(data[, "DateTime"], c(date2, date1, base_date + ((-234.7239 - 1) * 86400), base_date + ((29384723 - 2) * 86400), NA, NA, NA, base_date + ((14 - 1) * 86400), base_date + ((1 - 1) * 86400))))
stopifnot(compare(data[, "Boolean"], c(TRUE, TRUE, TRUE, FALSE, TRUE, FALSE, NA, FALSE, TRUE)))
stopifnot(compare(data[, "Formula"], c(14, 1, 43922.5674, 44197, -234.7239, 29384723, NA, NA, NA)))
stopifnot(compare(data[, "Error"], c(NA, 14, 1, 43922.5674, 44197, -234.7239, 29384723, NA, NA)))
