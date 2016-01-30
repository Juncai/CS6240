# Load Salaries.csv from current directory
salaries <- read.csv(file="Salaries.csv", head=TRUE)
# Subsetting the salaries data frame with only "playerID", "yearID", "teamID", "salary"
sCols <- c("playerID", "yearID", "teamID", "salary")
salaries <- salaries[sCols]

# Load Salaries.csv from current directory
batting <- read.csv(file="Batting.csv", head=TRUE)
# Subsetting the salaries data frame with only "playerID", "yearID", "teamID", "HR"
bCols <- c("playerID", "yearID", "teamID", "HR")
batting <- batting[bCols]

# Join the two data frames on "playerID", "yearID", "teamID"
res <- merge(salaries, batting, by=c("playerID", "yearID", "teamID"))
Salary <- res$salary
Homerun <- res$HR
# Plot salary VS hr
require(ggplot2)
#qplot(res$salary, res$HR)
qplot(Salary, Homerun)

# Save the result graph as a PNG file
ggsave("SalaryVSHomerun.PNG")

# Start of EXTRA work
require(plyr)
team_msalary <- ddply(salaries,~teamID,summarise,meanS=mean(salary))
team_mhr <- ddply(batting,~teamID,summarise,meanH=mean(HR))
extra_res1 <- merge(team_mhr, team_msalary, by="teamID")
qplot(extra_res1$meanS, extra_res1$meanH)

