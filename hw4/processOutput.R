options(warn=-1)
suppressMessages(library(plyr))

res <- data.frame(DISTANCE=integer(),
                  AIR_TIME=integer(), 
                  AVG_TICKET_PRICE=double(), 
                  stringsAsFactors=FALSE) 
input_path <- Sys.getenv("MR_INPUT")
filenames <- list.files(input_path, pattern="*.csv.gz", full.names=TRUE)
for (i in 1:length(filenames))
	res <- rbind(res, read.csv(file=filenames[i], head=TRUE, row.names=NULL)[c("DISTANCE", "AIR_TIME", "AVG_TICKET_PRICE")])

# testing the model?

# plot the graphs 
# read results from output
res <- data.frame(month=character(),
                  carrier=character(), 
                  totalFl=integer(), 
				          meanPrice=double(),
                  stringsAsFactors=FALSE) 

filenames <- list.files("output", pattern="part-r-*", full.names=TRUE)
for (i in 1:length(filenames))
	res <- rbind(res, read.csv(file=filenames[i], head=FALSE))


require(plyr)
carrier_flnum <- ddply(res,~carrier,summarise,totalFL=mean(totalFl))
attach(carrier_flnum)
sorted_carrier <- carrier_flnum[order(totalFL, decreasing=TRUE),]
top_carriers <- sorted_carrier[1:10,]
top_carriers <- top_carriers$carrier
detach(carrier_flnum)

for (i in 1:10)
{
	c_carrier <- top_carriers[i]
	c_df <- res[res$carrier == c_carrier,]
	attach(c_df)
	c_sorted <- c_df[order(month),]
	detach(c_df)
	library(MASS)
	names(c_sorted) <- NULL
	#print(c_sorted[2,c("month", "carrier", "meanPrice")], row.names = FALSE)
	write.table(c_sorted[,c(1, 2, 4)], quote = FALSE, row.names = FALSE)
	# lines(c_sorted$month, c_sorted$meanPrice, col=colcolors[i], pch=pchs[i], lty=ltys[i], type="o")
}


