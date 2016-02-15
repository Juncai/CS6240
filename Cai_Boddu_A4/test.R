options(warn=-1)
suppressMessages(library(plyr))

# load stats
stats <- read.csv(file="tmp/stats", head=FALSE, row.names=NULL)
mean_d <- stats[1, 1]
mean_t <- stats[1, 2]
std_d <- stats[2, 1]
std_t <- stats[2, 2]

# load MR output
# and get the unique carriers
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

res <- data.frame(UNIQUE_CARRIER=character(),
				  DISTANCE=integer(),
                  AIR_TIME=integer(), 
                  AVG_TICKET_PRICE=double(), 
                  stringsAsFactors=FALSE) 
input_path <- Sys.getenv("MR_INPUT")
filenames <- list.files(input_path, pattern="*.csv.gz", full.names=TRUE)
for (i in 1:length(filenames)) {
#	res <- rbind(res, read.csv(file=filenames[i], head=TRUE, row.names=NULL)[c("DISTANCE", "AIR_TIME")])
	res <- rbind(res, read.csv(file=filenames[i], head=TRUE, row.names=NULL)[c("UNIQUE_CARRIER", "DISTANCE", "AIR_TIME", "AVG_TICKET_PRICE")])
}
names(res) <- c("carrier", "distance", "time", "price")


# mse map
mse_results <- data.frame(carrier <- character(),
						  d_mse <- double(),
						  t_mse <- double())

#res <- res[complete.cases(res),]
res$carrier <- factor(res$carrier, levels=levels(carriers))
for (i in 1:length(carriers)) {
	c_res <- res[res$carrier == carriers[i],]
	c_res <- na.omit(c_res)
# mse for air distance
	ct0 <- thetas[thetas$carrier == carriers[i] & thetas$feature == "D", 3]
	ct1 <- thetas[thetas$carrier == carriers[i] & thetas$feature == "D", 4]

	c_res$distance <- c_res$distance - mean_d
	c_res$distance <- c_res$distance / std_d
	c_res$distance <- c_res$distance * ct1
	c_res$distance <- c_res$distance + ct0
	c_res$d_error <- (c_res$distance - c_res$price) ^ 2
	d_mse <- mean(c_res$d_error)
# mse for air time
	ct0 <- thetas[thetas$carrier == carriers[i] & thetas$feature == "T", 3]
	ct1 <- thetas[thetas$carrier == carriers[i] & thetas$feature == "T", 4]

	c_res$time <- c_res$time - mean_t
	c_res$time <- c_res$time / std_t
	c_res$time <- c_res$time * ct1
	c_res$time <- c_res$time + ct0
	c_res$t_error <- (c_res$time - c_res$price) ^ 2
	t_mse <- mean(c_res$t_error)
	mse_results <- rbind(mse_results, data.frame(c(as.character(carriers[i])), c(d_mse), c(t_mse)))
}
mse_results
write.table(mse_results, "mse.csv", quote=FALSE, row.names=FALSE, col.names=FALSE, na="", sep=",")
