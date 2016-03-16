
args = commandArgs(trailingOnly=TRUE)
#input <- args[1]
output <- args[1]

# declare factors

#rawRes <- read.csv(input)
#unique(rawRes$destState)
#fc <- file(output)
for (i in 1:10) {
#	writeLines(paste(i, 'haha', sep=","), fc)
	write(paste(i, 'haha', sep=","), file=output, append=TRUE)
}
#close(fc)
