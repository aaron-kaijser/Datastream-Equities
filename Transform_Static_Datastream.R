# Script and explanations can be found here: https://github.com/aaron-kaijser/Datastream-Equities
# Author: Aaron Kaijser

rm(list=ls())
gc()

library(data.table)
library(stringr)

data <- fread("https://raw.githubusercontent.com/aaron-kaijser/Datastream-Equities/main/NL_Static_Request_WSCOPENL.csv")
data2 <- fread("https://raw.githubusercontent.com/aaron-kaijser/Datastream-Equities/main/NL_Static_Request_FHOL.csv")
data3 <- fread("https://raw.githubusercontent.com/aaron-kaijser/Datastream-Equities/main/NL_Static_Request_DEADNL.csv")

transform_static <- function(data) {
  # Data wrangling
  col_names <- as.vector(t(data[2,])) # saves second row as vector which we will use as column names
  data <- data[-c(1:2),] # removes first two rows
  setnames(data, old = colnames(data), new = col_names) # changes column names
  
  # Applying filters
  # 1. Major = Y
  data <- data[`MAJOR FLAG` == "Y"]
  # 2. Type = EQ
  data <- data[`STOCK TYPE` == "EQ"]
  # 3. ISINID = P
  data <- data[`QUOTE INDICATOR` == "P"]
  # 4. GEOGN = Country
  data <- data[`GEOGRAPHIC DESCR.` == "NETHERLANDS"]
  # 5. GEOLN = Country
  data <- data[`GEOG DESC OF LSTNG` == "NETHERLANDS"]
  # 6. PCUR = E or FL
  data <- data[`CURRENCY` == "E" | `CURRENCY` == "FL"] # FL was The Netherlands' currency prior to the euro (E)
  # 7. GGISN = Country
  data <- data[`ISIN ISSUER CTRY` == "NL"]
  # 8. NAME/ENAME/ECNAME that are often linked to non-common stock must be removed
  keyword_filter = c(
    "1000DUPL", "DULP", "DUP", "DUPE", "DUPL", "DUPLI", "DUPLICATE", "XSQ", "XETa", "ADR", "GDR", "PF", "'PF'", "PFD", "PREF", "PREFERRED", "PRF", "WARR", "WARRANT", "WARRANTS", "WARRT", "WTS", "WTS2", "%", "DB", "DCB", "DEB", "DEBENTURE", "DEBENTURES", "DEBT", ".IT", ".ITb", "TST", "INVESTMENT TRUST", "RLST IT", "TRUST", "TRUST UNIT", "TRUST UNITS", "TST", "TST UNIT", "TST UNITS", "UNIT", "UNIT TRUST", "UNITS", "UNT", "UNT TST", "UT", "AMUNDI", "ETF", "INAV", "ISHARES", "JUNGE", "LYXOR", "X-TR", "EXPD", "EXPIRED", "EXPIRY", "EXPY", "ADS", "BOND", "CAP.SHS", "CONV", "DEFER", "DEP", "DEPY", "ELKS", "FD", "FUND", "GW.FD", "HI.YIELD", "HIGH INCOME", "IDX", "INC.&GROWTH", "INC.&GW", "INDEX", "LP", "MIPS", "MITS", "MITT", "MPS", "NIKKEI", "NOTE", "OPCVM", "ORTF", "PARTNER", "PERQS", "PFC", "PFCL", "PINES", "PRTF", "PTNS", "PTSHP", "QUIBS", "QUIDS", "RATE", "RCPTS", "REAL EST", "RECEIPTS", "REIT", "RESPT", "RETUR", "RIGHTS", "RST", "RTN.INC", "RTS", "SBVTG", "SCORE", "SPDR", "STRYPES", "TOPRS", "UTS", "VCT", "VTG.SAS", "XXXXX", "YIELD", "YLD", "CERTIFICATE", "CERTIFICATES", "CERT", "CERTS", "CERTIFICATES\\\\", "STK\\\\"
  ) # very long keyword filter vector
  # Checks whether these variables contain any of the strings in the keyword filter and removes these observations
  for (i in 1:length(keyword_filter)) {
    data <- data[!`NAME` %like% keyword_filter[i]]
    data <- data[!`FULL NAME` %like% keyword_filter[i]]
    data <- data[!`COMPANY NAME` %like% keyword_filter[i]]
  }
  
  return(data$`ISIN CODE`) # replace `ISIN CODE` by TYPE if you would like to obtain a vector of Datastream identifiers instead of ISIN identifiers. I would suggest ISIN codes so you can look up the firm later. 
  # return(data[, list(`ISIN CODE`, `COMPANY NAME`)]) # use this line if you would like to return a dataframe with the company names + ISIN codes
}

# Combining all vectors and selecting unique codes only
final_codes <- unique(c(transform_static(data), transform_static(data2), transform_static(data3))) 
# final_codes <- unique(rbind(transform_static(data), transform_static(data2), transform_static(data3)), by = "ISIN CODE")

# Saving as .txt file
write.table(final_codes, "C:/NL_final_codes.txt", quote = F, row.names = F, col.names = F)


