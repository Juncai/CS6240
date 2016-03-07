res <- data.frame(DISTANCE=integer(),
                  AIR_TIME=integer(), 
                  stringsAsFactors=FALSE) 
input_path <- Sys.getenv("MR_INPUT")
filenames <- list.files(input_path, pattern="*.csv.gz", full.names=TRUE)
for (i in 1:length(filenames))
	res <- rbind(res, read.csv(file=filenames[i], head=TRUE, row.names=NULL)[c("DISTANCE", "AIR_TIME")])

multi.fun <- function(x) {
	c(mean = mean(x, na.rm=TRUE), sd = sd(x, na.rm=TRUE), min = min(x, na.rm=TRUE), max = max(x, na.rm=TRUE))
}

stats <- sapply(res, multi.fun)
write.table(stats, "tmp/stats", quote=FALSE, row.names=FALSE, col.names=FALSE, na="", sep=",")

library(randomForest)
x <- cbind(x_train,y_train)
# Fitting model
fit <- randomForest(Species ~ ., x,ntree=500)
summary(fit)
#Predict Output 
predicted= predict(fit,x_test)
