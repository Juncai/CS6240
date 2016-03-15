library(randomForest)
library(methods)
filename <- "/tmp/OTP_prediction.rf"
rfString <- readChar(filename, file.info(filename)$size)
rf <- unserialize(charToRaw(rfString))
as(rf, "randomForest")
#print(rf)
