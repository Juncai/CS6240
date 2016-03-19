library(randomForest)
library(methods)

getForest <- function(path) {
	rfString <- readChar(path, file.info(path)$size)
	rf <- unserialize(charToRaw(rfString))
	as(rf, "randomForest")
}

args = commandArgs(trailingOnly=TRUE)
input <- args[1]
rf_path <- args[2]
output <- args[3]

# declare facotrs
quarterF <- c(1:4)
monthF <- c(1:12)
dayOfMonthF <- c(1:31) 
dayOfWeekF <- c(1:7) 
carrierF <- c('9E','AA','AQ','AS','B6','CO','DH','DL','EA','EV','F9','FL','HA','HP','ML (1)','MQ','NK','NW','OH (1)','OO','PA (1)','PI','PS (1)','TW','TZ','UA','US','VX','WN','XE','YV')
isHolidayF <- c(0, 1) 
stateF <- c(1,2,4,5,6,8,9,10,12,13,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,53,54,55,56,72,75,78)
distanceGroupF <- c(1:11)
depHourOfDayF <- c(0:23) 
arrHourOfDayF <- c(0:23)
elapsedTimeInHoursF <- c(0:20)
isDelayF <- c(0, 1)

rawRes <- read.csv(input);
# use 'state' instead of airport or city
#x <- subset(rawRes, select = -c(originAI, originCity, destAI, destCity, isDelay))
res <- subset(rawRes, select = -c(originAI, origin, destAI, dest, flNum, flDate, crsDepTime, elapsedTime, depMS, arrMS))
res$quarter <- factor(res$quarter, levels=quarterF, ordered=TRUE)
res$month <- factor(res$month, levels=monthF, ordered=TRUE)
res$dayOfMonth <- factor(res$dayOfMonth, levels=dayOfMonthF, ordered=TRUE)
res$dayOfWeek <- factor(res$dayOfWeek, levels=dayOfWeekF, ordered=TRUE)
res$carrier <- factor(res$carrier, levels=carrierF, ordered=FALSE)
res$isHoliday <- factor(res$isHoliday, levels=isHolidayF, ordered=TRUE)
res$originState <- factor(res$originState, levels=stateF, ordered=TRUE)
res$destState <- factor(res$destState, levels=stateF, ordered=TRUE)
res$distanceGroup <- factor(res$distanceGroup, levels=distanceGroupF, ordered=TRUE)
res$depHourOfDay <- factor(res$depHourOfDay, levels=depHourOfDayF, ordered=TRUE)
res$arrHourOfDay <- factor(res$arrHourOfDay, levels=arrHourOfDayF, ordered=TRUE)
res$elapsedTimeInHours <- factor(res$elapsedTimeInHours, levels=elapsedTimeInHoursF, ordered=TRUE)
res$isDelay <- factor(res$isDelay, levels=isDelayF, ordered=TRUE)

#for (n in names(x)) {
#	x[[n]] <- factor(x[[n]])
#}

x <- subset(res, select = -c(isDelay))
#y <- res$isDelay
set.seed(687)

rf <- getForest(rf_path)
prediction <- predict(rf, x, type="response", norm.votes=TRUE, predict.all=FALSE, proximity=FALSE, nodes=FALSE)
#prediction <- c(1, 0)
for (i in 1:length(rawRes$originAI)) {
#	print(paste(paste(rawRes$originAI[i], rawRes$originCity[i], rawRes$destAI[i], sep="_", collapse=NULL), prediction[i], sep=",", collapse=NULL))
	write(paste(rawRes$carrier[i], paste(rawRes$flDate[i], rawRes$depMS[i], rawRes$arrMS[i], rawRes$originAI[i], rawRes$origin[i], rawRes$destAI[i], rawRes$dest[i], rawRes$flNum[i], rawRes$elapsedTime[i], prediction[i], sep="_", collapse=NULL), sep=",", collapse=NULL), file=output, append=TRUE)
}
#rfString <- rawToChar(serialize(fit, NULL, ascii=TRUE))
#write(rfString, file = "/tmp/OTP_prediction.rf")
#write(rfString, file = output)
