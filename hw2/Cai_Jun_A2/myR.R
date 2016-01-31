setwd("./output")
res <- read.csv(file="part-r-00000", head=FALSE)
names(res) <- c("month", "carrier", "totalFl", "meanPrice")
# attach(res)
# sorted_res <- res[order(month, -meanPrice),]
# sorted_res <- sorted_res[!is.na(totalFl)]

# find the top 10 carriers with most flights
require(plyr)
carrier_flnum <- ddply(res,~carrier,summarise,totalFL=mean(totalFl))
attach(carrier_flnum)
sorted_carrier <- carrier_flnum[order(totalFL, decreasing=TRUE),]
top_carriers <- sorted_carrier[1:10,]
top_carriers <- top_carriers$carrier
# sorted_res <- sorted_res[carrier %in% top_carriers,]

# loop through top carriers and plot the mean prices for each month
c_carrier <- top_carriers[1]
c_df <- res[res$carrier == c_carrier,]
attach(c_df)
c_sorted <- c_df[order(month),]
require(ggplot2)
qplot(c_sorted$month, c_sorted$meanPrice)
ggsave("test.PNG")

