res <- data.frame(DISTANCE=integer(),
                  AIR_TIME=integer(), 
                  stringsAsFactors=FALSE) 
#input_path <- Sys.getenv("MR_INPUT")
input_path <- "/home/jon/Downloads/part"
pattern <- "^201[01234]"
filenames <- list.files(input_path, pattern="*.csv.gz", full.names=TRUE)
for (i in 1:length(filenames))
	c_res <- read.csv(file=filenames[i], head=TRUE, row.names=NULL)	
	res <- rbind(res, c_res[grepl(pattern, c_res$FL_DATE),][c("DISTANCE", "AIR_TIME")]
)

multi.fun <- function(x) {
	c(mean = mean(x, na.rm=TRUE), sd = sd(x, na.rm=TRUE), min = min(x, na.rm=TRUE), max = max(x, na.rm=TRUE))
}

stats <- sapply(res, multi.fun)
write.table(stats, "tmp/stats_new", quote=FALSE, row.names=FALSE, col.names=FALSE, na="", sep=",")

