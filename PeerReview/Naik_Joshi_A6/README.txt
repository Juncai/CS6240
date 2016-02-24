--------------------------------------------
README for Assignment 5 : Missed Connections
--------------------------------------------
Team members: Vedant Naik, Rohan Joshi

---------
CONTENTS:
---------
The project folder/zip contains the following files
- readme.txt
- src
--- FileRecord.java
--- MissedConnections.java
- run_aws.sh
- cloud.sh
- script.cfg
- Makefile
- all		(empty data folder, you should place the .csv.gz files here)

-------------------------
Requirements for project:
-------------------------
- Linux Environment
- Java 1.7+
- Hadoop 2.7.1+
- Text editor to view the files or any IDE
- jq: a tool to grep json data		(required only for EMR)

-----------------
Reference labels:
-----------------
- <BUCKET_NAME> : The name of the S3 bucket
- <DOWNLOAD_PATH> : The path of the folder where the output files are download
- <HADOOP_HOME> : The path of folder where hadoop lives in your system

---------------------
SOLUTION DESCRIPTION:
---------------------

Mapper:
Key	:	Initially we wanted flights of the same carrier and same year
		so we proposed a key like this
					<CARRIER><YEAR>
		In order to get the flights that are candidates for "connections", we add the airport ID
		i.e. Origin and destination, to the key as well.			
		To achieve this, for each record in mapper, write two values to the context, one with origin
		and other with destination.
		Values are marked using the connectionTpye filed in the AirlineMapperValue object.
		- so now, in reducer, we will have list of flights grouped as
				<CARRIER> <YEAR> <AIRPORT> 
								origin/dest 

Partitioner:		
	Since the choice of key structure in mapper will lead to a lot of keys, we could have a partitioner that 
	sends values of same carriers to the same Reducer. I.E. based on <CARRIER> alone.

AirlineMapperValue:	
	We will need the following from each flight record:
	ConnectionType: Which will mark this value as an ORIGIN or DESTINATION
	Scheduled arrival and departure times
	Actual arrival and departure times
	[NOTE: store the times in long, so that difference can be calculated for connections that span over
			days and months as mentioned in piazza post @77]

-----------------------------------------------------
How to run the project in the pseudo-distributed mode
-----------------------------------------------------
- Ensure that the local hadoop is running
- Make sure HADOOP_HOME is set to the right path
- If your hadoop version is not 2.7.2, update the version number in Makefile
- Type jps and ensure datanode, namenode and secondarynamenode are up and running
- Download the tar.gz project
- Unzip the files in the your file system
- Navigate to the project directory using the terminal
- Ensure that the Makefile is present
- In the terminal type: 
	make pseudo

-----------------------------------------
What happens when you type 'make pseudo':
-----------------------------------------
- The jar is created using the source code
- A new input directory inside your HDFS is created
- The data source is copied inside the input directory under the newly created user in HDFS
- The job is then started using the jar and the input that was just copied
- The output is then copied to a folder named outputPseudo in the project directory
- Time taken to run the code is stored in ${HADOOP_HOME}/pseudoTime.csv
- Output is displayed on the terminal	

--------------------------------------------------------
Requirements for the project to run successfully on AWS:
--------------------------------------------------------
- The CLI should be configured correctly so that it can run commands and interact with your AWS account
- The script that runs the job on AWS will show the progress, please make sure you have the terminal window open till the script completes
- It is assumed that the reviewer will run the job on his own AWS account to prevent the exchange of keys
- Please make sure that the output format for AWS CLI is set to 'json' before running the program in AWS mode
- *** the download path for the folder where the script downloads the AWS output files is relative to the directory in which you extracted the project ***
- please ensure that you are in the project directory when you run the commands (i.e, you have cd)

--------------------------------------------------
Details about the script that runs the job on AWS:
--------------------------------------------------
- The bucket name on S3 needs to be unique.  If you get an error saying that the bucketname exists then please change the bucket name in the script.cfg file
and try again
- The script contains comments that make it easier to understand the code
- There are respective commands that install the jq tool based on the OS you are using. Please uncomment the one that corresponds to your OS

-------------------------------------------------------------------
Where do you have to plug in the configurations of your own system:
-------------------------------------------------------------------
- You will have to specify the local path where the log and output files will be downloaded in the script.cfg file. The places are marked with <DOWNLOAD_PATH>
- You will have to specify your bucket name on S3 in the script.cfg file.  The places are marked with <BUCKET_NAME>

---------------------------------------
Instructions to run the program on AWS:
---------------------------------------
- run the 'make emr' command
	- The script pulls the bucketname from the script.cfg file
	- Using this bucket name, a new bucket will be created on AWS
	- The input files from 'all' will be copied to the bucket/input/all folder
	- The jar is then copied from the project folder into the S3 bucket
	- The cluster is then started with the jar, input/all folder and the output is put into the bucket/output folder
	- The script keeps polling for the status, and when the status turns to 'TERMINATED', the output files from AWS are copied into outputAws

---------
ANALYSIS:
---------

We think, in this assignment, we have more intensive processing on the reducer side.
Initially, we used <CARRIER>-<YEAR> as our key from the mapper. This meant, we had to check if the 
origin and destination of 2 flight records match, on the reducer. Since any flight can be F or G (as described 
in the problem statement) this led to checking each value with every other value (n^2 comparisons).

We reduced the amount of comparisons done on reducer side by adding the airport id in the mapper key, 
and writing two Key:Value pairs form the mapper. Values are marked as "ORIGIN" or "DESTINATION".
This gave us a list of values in the reducer (for each key), such that all values were of the same carrier, in
the same year, and had matching origin and destination airports. So, only time comparisons were left for the reducer.

-----------------
REFERENCE OUTPUT:
-----------------

Output is in the following format:

CARRIER	YEAR	CONNECTIONS	MISSEDCONNECTIONS	MISSEDCONNECTIONSPERCENT

FL	2014	9299936.0		383032.0	4.118651999325587
VX	2013	3978501.0		222709.0	5.597811839182647
MQ	2013	1.33230313E8	7299681.0	5.478994108495415
MQ	2015	688052.0		38536.0	 	5.600739478992867
MQ	2014	1.04610673E8	6306876.0	6.028902997306976
FL	2013	3.0017868E7		1771472.0	5.901391797711949
DL	2013	5.03988325E8	1.8345504E7	3.640065273337433
US	2013	1.3996969E8		4469803.0	3.1934078013604235
DL	2015	3836174.0		95113.0	 	2.4793713736655327
DL	2014	5.65549891E8	1.889312E7	3.3406637152017415
VX	2014	3990708.0		210255.0	5.268613990299466
VX	2015	25446.0			796.0	 	3.1281930362335926
HA	2015	91333.0			3337.0	 	3.6536629695728817
US	2014	1.37766079E8	4561272.0	3.310881773734738
HA	2014	1.2425623E7		348796.0	2.807070518717653
US	2015	937179.0		28640.0	 	3.0559797007828813
HA	2013	1.1889507E7		289521.0	2.4350967622122597
AA	2013	2.66070974E8	9610051.0	3.6118374189888147
AA	2014	2.72070307E8	1.1765746E7	4.324524101779324
AA	2015	1746964.0		63053.0	 	3.609290174268044
AS	2013	1.8729921E7		606798.0	3.239725357090401
AS	2014	2.019394E7		722842.0	3.579499592451993
AS	2015	135613.0		4528.0	 	3.3389129360754497
YV	2013	1.4914307E7		776212.0	5.204479162189701
UA	2015	788657.0		29019.0	 	3.6795463680662186
B6	2014	3.2274116E7		1836407.0	5.690030363651168
UA	2014	1.2928204E8		5930185.0	4.587013787839362
B6	2015	225131.0		10553.0	 	4.687493059596413
UA	2013	1.34802869E8	5628476.0	4.175338434377091
B6	2013	3.1781938E7		1955151.0	6.151767711585115
WN	2014	3.2368359E8		1.1557354E7	3.570571495453322
NK	2015	34550.0			1608.0	 	4.6541244573082485
WN	2013	3.12716985E8	9546223.0	3.0526717312780436
WN	2015	2326906.0		60942.0	 	2.6190142618567314
EV	2014	1.70373409E8	9849785.0	5.781292431614138
EV	2013	2.18771375E8	1.2029829E7	5.498813087406887
F9	2014	1.1395571E7		739536.0	6.489679192029957
EV	2015	967322.0		47569.0	 	4.917597242696847
F9	2013	1.2343575E7		709532.0	5.748188835082218
9E	2013	6.7972001E7		2879239.0	4.235919139705774
OO	2014	1.28823649E8	6573299.0	5.102556130823464
OO	2015	809660.0		48425.0	 	5.980905565299014
OO	2013	1.4148913E8		6832988.0	4.829337773156142
F9	2015	38548.0			2920.0	 	7.574971464148595

-----------
CONCLUSION:
-----------

We get percentage of missed connections in the range of 2.4% to 7.6%.
From this we can conclude that F9 in the year of 2015 had the highest percentage of missed connections.
And HA in the year of 2013 had the lowest percentage of missed connections.

--------------------------------------------------------------------------------------------------------
