res <- data.frame(month=character(),
                 carrier=character(), 
                 totalFl=integer(), 
				 meanPrice=double(),
                 stringsAsFactors=FALSE) 

filenames <- list.files("output", pattern="part-r-*", full.names=TRUE)
for (i in 1:length(filenames))
	res <- rbind(res, read.csv(file=filenames[i], head=FALSE))
# setwd("./output")
# res <- read.csv(file="part-r-00000", head=FALSE)
names(res) <- c("month", "carrier", "totalFl", "meanPrice")
res <- res[complete.cases(res),]
length(res)
summary(res)
min(res[,4])
# min(res[,4], na.rm=TRUE)
