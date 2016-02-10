res <- data.frame(DISTANCE=integer(),
                  AIR_TIME=integer(), 
                  stringsAsFactors=FALSE) 
input_path <- Sys.getenv("MR_INPUT")
filenames <- list.files(input_path, pattern="*.csv.gz", full.names=TRUE)
for (i in 1:length(filenames))
	res <- rbind(res, read.csv(file=filenames[i], head=TRUE, row.names=NULL)[c("DISTANCE", "AIR_TIME")])
# summary(res)
sapply(res, mean, na.rm=TRUE)
sapply(res, sd, na.rm=TRUE)

