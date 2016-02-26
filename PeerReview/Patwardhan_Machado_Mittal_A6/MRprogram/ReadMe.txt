READ ME: 
===================================================================================================================================================================================
Understanding the Program and its Flow:
===================================================================================================================================================================================
Arguments: Enter the arguments.

Argument 0: The path where all the input files are present. To run the program for inputs of various sizes, create new input directories of 
			different sizes and change them accordingly in the Make file. The path given as args[0] gives the directory where all the files are present.

Argument 1: The argument is given as the path where the output should be stored.
	
The mapper begins reading each line of the sample data set provided to it and passes it to the sanity test. If the data passes the sanity test, 
get the scheduled arrival and departure time, actual arrival and departure time along with the carrier name and year for each record and create 
a custom object with all the values.

The mapper then sends the airline and the year as a key along with the custom object to the reducer.

The reducer takes the values for each airline and the year as a key. 

The reducer computes for connections between flights and if there is a connection, increments its counter. Similarly we compute 
the number of misconnections too.

The output is then printed as carrier, year, number of misconnections and percentage.


==========================================================================================================
Requirements:
==========================================================================================================
Linux or OS X
jq (for emr)
jdk 1.7+

==========================================================================================================
How to run
==========================================================================================================
There is Makefile using which you can run the program.
Our main file is HW05.java. For compilation please replace hadoop jar versions according to your hadoop version. 
My version of hadoop is 2.6.3

Command : make pseudo : This command will run the program on pseudo distributed environment. Get the output from hdfs and
		  run the Rscript on it. 
		  
		  In make pseudo you need to mention proper folder paths for input/output according to your file system.
		  
		  Following commands inside the make pseudo needs input/output folder paths.
		  
		  1)hadoop dfs -copyFromLocal <source of input files> <destination in hdfs>
		  2)hadoop jar job.jar HW05 <hdfs input path> <hdfs output path>
		  3)hadoop fs -get <hdfs output path>
		
   
==========================================================================================================================================================================================================
Output: 
==================================================================================================================================================================
We ran the program on file a.csv.gz and b.csv.gz and this is the output we received.

UA 2013 501 6.45

==========================================================================================================================================================================================================
Conclusions:
==========================================================================================================================================================================================================		

We learnt that an O(n^2) solution for a map reduce program is not feasible and there has to be more feasible ways to 
perform joins on a table joined with itself.
