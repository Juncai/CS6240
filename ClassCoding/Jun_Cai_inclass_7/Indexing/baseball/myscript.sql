drop table if exists Salaries;
drop table if exists Batting;
drop table if exists Master;
drop table if exists Teams;

.mode csv
.head on
.mode column
.import ./Salaries.csv Salaries
.import ./Batting.csv Batting
.import ./Master.csv Master
.import ./Teams.csv Teams

update Batting set hr = CAST(hr as REAL);
update Salaries set salary = CAST(salary as REAL);

select b.playerID as PlayerID, m.nameFirst as FirstName, m.nameLast as LastName, m.nameGiven as GivenName, (b.hr / s.salary) as HRPerDollar 
from Batting b 
inner join Salaries s 
on b.yearID=s.yearID and b.playerID = s.playerID and b.teamID=s.teamID 
inner join Master m
on b.playerID=m.playerID
where b.yearID='2009' 
order by HRPerDollar desc
limit 1; 


select b.teamID as TeamID, t.name as TeamName, (sum(s.salary) / sum(b.hr)) as DollarPerHR 
from Batting b 
inner join Salaries s 
on b.yearID=s.yearID and b.playerID=s.playerID and b.teamID=s.teamID 
inner join Teams t
on t.teamID=b.teamID
where b.yearID='2008'
group by b.teamID 
order by DollarPerHR desc 
limit 1; 

