#iterary <- data.frame(year=integer(),
#					 month=integer(),
#					 day=integer(),
#					 origin=character(),
#					 dest=character(),
#					 flNum1=integer(),
#					 flNum2=integer(),
#					 duration=integer(),
#					 stringsAsFactors=TRUE)
#iteraryDir <- 'output'
args = commandArgs(trailingOnly=TRUE)
iteraryDir <- args[1]
validatePath <- args[2]


filenames <- list.files(iteraryDir, pattern="part-r-*", full.names=TRUE)
totalDuration <- 0
if (length(filenames) > 0) {
	iteraryStrings <- readLines(con <- file(filenames[1]))
	if (length(filenames) > 1) {
		for (i in 2:length(filenames)){
			cStrings <- readLines(con <- file(filenames[i]))
			append(iteraryStrings, cStrings, after = length(iteraryStrings))
#	iterary <- rbind(iterary, read.csv(file=filenames[i], head=FALSE, row.names=NULL))
		}
	}
	validateStrings <- readLines(con <- file(validatePath))
	for (i in 1:length(iteraryStrings)) {
		g <- regexpr(",[^,]*$", iteraryStrings[i])
		key <- substr(iteraryStrings, 1, g[1] - 1) 
		value <- as.numeric(substr(iteraryStrings, g[1] + 1, nchar(iteraryStrings[i]))) 
		totalDuration <- totalDuration + value
		if (key %in% validateStrings) {
			totalDuration <- totalDuration + 100 * 60
		}
	}
}
#names(iterary) <- c("year", "month", "day", "origin", "dest", "flNum1", "flNum2", "duration")

#validate <- data.frame(
print(totalDuration)
