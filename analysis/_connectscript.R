# Charity Data project raw R connection and database query function

library(DBI)
library(RPostgreSQL)

# Set up database connection and query to table function

setwd('C:\\Users\\willl\\PycharmProjects\\CharityData\\analysis')
setwd('..')

login <- read.csv('.\\lib\\pw.py', 
                  header = FALSE, 
                  sep = '=', 
                  stringsAsFactors = FALSE, 
                  strip.white=TRUE)
uname <- gsub('\'', '', login$V2[login$V1 %in% 'us'])
pword <- gsub('\'', '', login$V2[login$V1 %in% 'pw'])
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = 'CCEng',
                 host = 'localhost', port = 5432,
                 user = uname, password = pword)
rm(uname)
rm(pword)
rm(login)

runquery <- function(connection = con, sql) {
  res <- dbSendQuery(connection, sql)
  toreturn <- dbFetch(res)
  dbClearResult(res)
  return(toreturn)
}