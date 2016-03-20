# load MR output
#    quarter, month, dayOfMonth, dayOfWeek, carrier, isHoliday, originAI, originCity, originState,
#    destAI, destCity, destSate, distanceGroup, depHourOfDay, arrHourOfDay, elapsedTimeInHours, isDelay
 
#res <- data.frame(quarter=integer(),
#					 month=integer(),
#					 dayOfMonth=integer(),
#					 dayOfWeek=integer(),
#					 carrier=character(),
#					 isHoliday=integer(),
#					 originAI=integer(),
#					 originCity=integer(),
#					 originState=integer(),
#					 stringsAsFactors=FALSE)
args = commandArgs(trailingOnly=TRUE)
input <- args[1]
output <- args[2]

library(randomForest)
rfString <- "haha"
write(rfString, file = output)
