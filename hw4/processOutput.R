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
					 t0=double(),
					 t1=double(),
					 stringsAsFactors=FALSE)
input_path <- "output"
filenames <- list.files(input_path, pattern="part-r-*", full.names=TRUE)
for (i in 1:length(filenames))
	thetas <- rbind(thetas, read.csv(file=filenames[i], head=FALSE, row.names=NULL))
names(thetas) <- c("carrier", "feature", "t1", "t0")
carriers <- unique(thetas$carrier)
n_carriers <- length(carriers)

# plot the fitting graph
# loop through top carriers and plot the mean prices for each month
pdf("PricePredictions.pdf")
plot_counter <- 0
opar <- par(no.readonly=TRUE)
par(mfrow=c(3, 2))
colcolors <- rainbow(10)

x_d <- seq(min_d, max_d, length.out=5)
x_t <- seq(min_t, max_t, length.out=5)
for (i in 1:n_carriers)
{
	plot_counter <- plot_counter + 1
	ct0 <- thetas[thetas$carrier == carriers[i] & thetas$feature == "D", 3]
	ct1 <- thetas[thetas$carrier == carriers[i] & thetas$feature == "D", 4]
	y_d <- ct1 * ((x_d - mean_d) / std_d) + ct0

	plot(x_d, y_d, type="l",
		 main=carriers[i],
		 xlab="Distance/mile", ylab="Mean Ticket Price",
		 col="blue",
		 xlim=c(0, 6000),
		 ylim=c(0, 1200))
	text(x_d, y_d, round(y_d, 2))
	text(100, 1100, substitute(paste(theta[0], ": ", rct), list(rct=round(ct0, 2))), adj=0)
	text(100, 930, substitute(paste(theta[1], ": ", rct), list(rct=round(ct1, 2))), adj=0)

	ct0 <- thetas[thetas$carrier == carriers[i] & thetas$feature == "T", 3]
	ct1 <- thetas[thetas$carrier == carriers[i] & thetas$feature == "T", 4]
	y_t <- ct1 * ((x_t - mean_t) / std_t) + ct0
	plot(x_t, y_t, type="l",
		 main=carriers[i],
		 xlab="Air Time/minute", ylab="Mean Ticket Price",
		 col="green",
		 xlim=c(0, 800),
		 ylim=c(0, 1200))
	text(x_t, y_t, round(y_t, 2))
	text(10, 1100, substitute(paste(theta[0], ": ", rct), list(rct=round(ct0, 2))), adj=0)
	text(10, 930, substitute(paste(theta[1], ": ", rct), list(rct=round(ct1, 2))), adj=0)
}
par(opar)
