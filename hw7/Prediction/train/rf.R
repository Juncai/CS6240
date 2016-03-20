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

res <- read.csv(input);
# use 'state' instead of airport or city
res <- subset(res, select = -c(originAI, originCity, destAI, destCity, flNum, flDate, crsDepTime))
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

x <- subset(res, select = -c(isDelay))
y <- res$isDelay

#install.packages('randomForest', repos='http://cran.us.r-project.org')
library(randomForest)

set.seed(687)
fit <- randomForest(x, y=y, xtest=NULL, ytest=NULL, ntree=10, replace=TRUE, norm.votes=TRUE, keep.forest=TRUE, maxnodes=1024)
rfString <- rawToChar(serialize(fit, NULL, ascii=TRUE))
write(rfString, file = output)
