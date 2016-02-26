READ ME: 
=================================================================================================================
Understanding the Program and its Flow:
=================================================================================================================
1. Read the all the files present in the all folder into the origin data where the key is year, carrier and origin of the record

2. Similarly, read all the files from the folder into destination where the key is year, carrier and origin.

3. Since the keys for origin and the destination are the same, we can join the origin and destination

4. We created a map from the joined data and got the CRS Arrival time, CRS Departure time, Arrival time, Departure time of the 
   the origin and the destination
   
5. As the map goes through each iteration for the joined data, we incremented the number of connections and missed connections.

6. We then created another map with the key as carrier and year and the value of each missed connection and connection

7. Group by the key with each key i.e. carrier and year

8. We go through each key value pair and aggregate the number of counts and missed counts

9. We create a data which finally groups the percentage and the number of missed counts

==========================================================================================================
Requirements:
==========================================================================================================
Linux or OS X
scala
spark

==========================================================================================================
How to run
==========================================================================================================
Extract the tar gzip

Run : "make run" to run the program

When you hit make run, it will ask you for options. Run MissedConnections.class

Our main object is MissedConnections and the scala file is missedConnections.scala
In the all folder, keep the files you wish to run the program on

Report: 
	To run the report, hit the command 'make report' or by typing following command: 
	Rscript -e "rmarkdown::render('Assignment06_Report.Rmd')" <path_of_output.csv>/output.csv
	make sure to edit paths to the input-output of R-script according to your folder structure before you
	hit the command
	
Input:
	We have a folder all, which contains a.csv and b.csv (Sample input files taken from professor Viteks course page)
	
There is MR program folder which contains all the make file, map reduce program  and read me from the previous assignment
==========================================================================================================
Output: 
==========================================================================================================
We are providing our previous assignment submission as well, so that if you want to compare timings of spark with Map Reduce
you can run our Map Reduce configuration as well. (Please follow the ReadMe provided with that project to run the program)

We ran the program on a.csv and b.csv(taken from Professor Viteks site)	on our code	

The output is generated in the format of carrier year number_of_missed_connections percentage
Spark Program:
UA 2013 173 6.02

We ran the time of the outputs on file 55.csv and recorded them to create the report 
Refer output.csv for time based comparisons report
==========================================================================================================
Conclusions:
==========================================================================================================		

We realised that spark is better at running programs in parallel over large scale data.
The what took us little over an hour to run a 55.csv on our hadoop map reduce program took us about 
20 minutes to run which lead to the conclusion that it is running much faster.
