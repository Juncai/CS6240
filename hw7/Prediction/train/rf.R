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
input_path <- "/tmp"
# filenames <- list.files(input_path, pattern="OTP_prediction_training_*.csv", full.names=TRUE)
filenames <- dir(input_path, pattern="OTP_prediction_training_*", full.names=TRUE)
tables <- lapply(filenames, read.csv)
res <- do.call(rbind, tables)
#length(unique(res$originAI))
#length(unique(res$originCity))
#length(unique(res$originState))
#length(unique(res$carrier))
# use 'state' instead of airport or city
res <- subset(res, select = -c(originAI, originCity, destAI, destCity))
for (n in names(res)) {
#	print(n)
#	print(length(unique(res[[n]])))
	res[[n]] <- factor(res[[n]])
}
#summary(res)

x <- subset(res, select = -c(isDelay))
y <- res$isDelay
#summary(y)

library(randomForest)
set.seed(687)
fit <- randomForest(x, y=y, xtest=NULL, ytest=NULL, ntree=1, replace=TRUE, norm.votes=FALSE, keep.forest=TRUE, maxnodes=1024)
#fit2 <- randomForest(x, y=y, xtest=NULL, ytest=NULL, ntree=1, replace=TRUE, norm.votes=FALSE, keep.forest=TRUE, maxnodes=1024)
#fit.all <- combine(fit1, fit2)
#prediction <- predict(fit.all, x)
#fit <- randomForest(x, y=y, xtest=x, ytest=y, ntree=10, replace=TRUE, norm.votes=FALSE, keep.forest=TRUE, maxnodes=1024)
#print(fit.all)
rfString <- rawToChar(serialize(fit, NULL, ascii=TRUE))
write(rfString, file = "/tmp/OTP_prediction.rf")
