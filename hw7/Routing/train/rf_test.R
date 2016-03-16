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
# filenames <- list.files(input_path, pattern="OTP_prediction_training_*.csv", full.names=TRUE)
#filenames <- dir(input_path, pattern="OTP_prediction_training_*", full.names=TRUE)
#tables <- lapply(filenames, read.csv)
#res <- do.call(rbind, tables)
# use 'state' instead of airport or city

#summary(y)

library(randomForest)
#fit2 <- randomForest(x, y=y, xtest=NULL, ytest=NULL, ntree=1, replace=TRUE, norm.votes=FALSE, keep.forest=TRUE, maxnodes=1024)
#fit.all <- combine(fit1, fit2)
#prediction <- predict(fit.all, x)
#fit <- randomForest(x, y=y, xtest=x, ytest=y, ntree=10, replace=TRUE, norm.votes=FALSE, keep.forest=TRUE, maxnodes=1024)
#print(fit.all)
rfString <- "haha"
#write(rfString, file = "/tmp/OTP_prediction.rf")
write(rfString, file = output)
