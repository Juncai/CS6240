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
input_path <- "/tmp"

res <- read.csv(input);
res <- subset(res, select = -c(originAI, originCity, destAI, destCity))
for (n in names(res)) {
	res[[n]] <- factor(res[[n]])
}
x <- subset(res, select = -c(isDelay))
y <- res$isDelay

library(randomForest)
set.seed(687)
fit <- randomForest(x, y=y, xtest=NULL, ytest=NULL, ntree=1, replace=TRUE, norm.votes=FALSE, keep.forest=TRUE, maxnodes=1024)
rfString <- rawToChar(serialize(fit, NULL, ascii=TRUE))
write(rfString, file = output)
