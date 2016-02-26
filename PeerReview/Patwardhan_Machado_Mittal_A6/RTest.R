require(dplyr)
require(ggplot2)
args <- commandArgs(TRUE) 
srcFile <- args[1]
outputFile <- args[2]
data <- read.csv(srcFile,header = FALSE,sep="\t")
names(data) <- c("TypeOfProgram","Time")
png(paste(outputFile,".png"))
ggplot(data=data, aes(x=data$TypeOfProgram, y=data$Time)) +
  geom_bar(stat="identity",fill="green",colour="black") + ggtitle("Time comparison of MR and Spark") +
  labs(x="Types of program",y="Execution time")

dev.off()
