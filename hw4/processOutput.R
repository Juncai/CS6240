options(warn=-1)
suppressMessages(library(plyr))

# load stats
stats <- read.csv(file="tmp/stats", head=FALSE, row.names=NULL)
mean_d <- stats[1, 1]
mean_t <- stats[1, 2]
std_d <- stats[2, 1]
std_t <- stats[2, 2]
min_d <- stats[3, 1]
min_t <- stats[3, 2]
max_d <- stats[4, 1]
max_t <- stats[4, 2]

# load MR output
thetas <- data.frame(carrier=character(),
					 feature=character(),
					 t1=double(),
					 t0=double(),
					 stringsAsFactors=FALSE)
input_path <- "output"
filenames <- list.files(input_path, pattern="part-r-*", full.names=TRUE)
for (i in 1:length(filenames))
	thetas <- rbind(thetas, read.csv(file=filenames[i], head=FALSE, row.names=NULL))

# plot the fitting graph
# loop through top carriers and plot the mean prices for each month
opar <- par(no.readonly=TRUE)
par(mfrow=c(2, 2))
# get min and max price, then config the y-axis
library(ggplot2)
colcolors <- rainbow(10)

c_carrier <- "EV"
c_feature <- "D"
x <- seq(min_d, max_d, length.out=100)
eq_d <- function(x) {
	2 * ((x - mean_d) / std_d) + 1
}
for (i in 1:4)
	#plot(x, eq_d(x), type="b")
	plot(x)
#	qplot(x, fun=eq_d, stat="function", geom="line", xlab="distance", ylab="price")
par(opar)





