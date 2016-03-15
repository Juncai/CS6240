library(randomForest)
library(methods)

getForest <- function(path) {
	rfString <- readChar(path, file.info(path)$size)
	rf <- unserialize(charToRaw(rfString))
	as(rf, "randomForest")
}

input_path <- "/tmp"
#filenames <- dir(input_path, pattern="OTP_prediction_forest_*", full.names=TRUE)
filenames <- list.files(input_path, pattern="OTP_prediction_forest_*", full.names=TRUE)
if (length(filenames) > 0) {
	forest <- getForest(filenames[1])
	if (length(filenames) > 1) {
		for (i in 2:length(filenames)) {
			nextForest <- getForest(filenames[i])
			forest <- combine(forest, nextForest)
		}
	}
	rfString <- rawToChar(serialize(rf, NULL, ascii=TRUE))
	write(rfString, file = "/tmp/OTP_prediction_final.rf")
}







#rfStrings <- lapply(filenames, readChar)
#res <- do.call(rbind, tables)

#filename <- "/tmp/OTP_prediction.rf"
#rfString <- readChar(filename, file.info(filename)$size)
#rf <- unserialize(charToRaw(rfString))
#as(rf, "randomForest")
#print(rf)


