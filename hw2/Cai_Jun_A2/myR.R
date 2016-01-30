setwd("./output")
res <- read.csv(file="part-r-00000", head=FALSE)
names(res) <- c("month", "carrier", "totalFl", "meanPrice")
# Start of EXTRA work
require(plyr)
team_msalary <- ddply(res,~teamID,summarise,meanS=mean(salary))
team_mhr <- ddply(batting,~teamID,summarise,meanH=mean(HR))
extra_res1 <- merge(team_mhr, team_msalary, by="teamID")
qplot(extra_res1$meanS, extra_res1$meanH)




