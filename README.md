Analyzing Vehicle Collisions and Traffic Violations in NYC


1.	DATA SOURCES

We have four Data Sources:

a.	NYPD Motor Vehicle Collisions : 
●	LINK : https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95

●	File name in directory: /datasets/NYPD_Motor_Vehicle_Collisions.csv

b.	Parking Violations Issued : 
●	LINK : https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2017/2bnn-yakx 

●	File name in directory: /datasets/Parking_Violations_Issued.csv

c.	Moving Violations Summonses : 
●	LINK : https://catalog.data.gov/dataset/traffic-tickets-issued-beginning-2008

●	File name in directory : /datasets/Traffic_Tickets_Issued.csv

d.	Weather data in New York City - 2016 : 
●	LINK : https://www.kaggle.com/mathijs/weather-data-in-new-york-city-2016

●	File name in directory : /datasets/weather_data_nyc_centralpark_2016.csv



2.	DATA INGESTION

We put the data files from the system’s memory to Dumbo local File system using tools like FUGU and FileZilla.
Next, the data was injected into Hadoop HDFS using commands mentioned in the file, 
‘/data_ingest/ingest.sql’

3.	ETL

The ETL related codes (including java files, class files and jar files) can be found in the /etl_code folder. The files can be executed using the following commands:


PARKING VIOLATIONS
●	javac -classpath `yarn classpath` -d . ParkingViolationETLMapper.java  -- to compile the java file (class file already provided)
●	javac -classpath `yarn classpath`:. -d . ParkingViolationETL.java  -- to compile the java file (class file already provided)
●	jar -cvf parkViolation.jar ParkingViolation*.class -- to create the jar file (file already provided)
●	hadoop jar parkViolation.jar ParkingViolationETL project/Parking_Violations_Issued.csv project/Parking_ETL_final_output   -- to execute the parking violation ETL MapReduce job


	MOTOR COLLISIONS
●	javac -classpath `yarn classpath` -d . MotorCollisionETL/MapperClass.java  -- to compile the java file (class file already provided)
●	javac -classpath `yarn classpath`:. -d . MotorCollisionETL/Cleanetl.java  -- to compile the java file (class file already provided)
●	jar -cvf Cleanetl.jar MotorCollisionETL/*.class -- to create the jar file (file already provided)
●	hadoop jar Cleanetl.jar Cleanetl /project/NYPD_Motor_Vehicle_Collisions.csv /project/Motor_Collisions_ETL_final_output


TRAFFIC TICKETS SUMMONSES
●	javac -classpath `yarn classpath` -d . TrafficTicketsIssuedETLMapper.java  -- to compile the java file (class file already provided)
●	javac -classpath `yarn classpath`:. -d . TrafficTicketsIssuedETL.java  -- to compile the java file (class file already provided)
●	jar -cvf TrafficTicketsIssuedETL.jar TrafficTicketsIssued*.class -- to create the jar file (file already provided)
●	hadoop jar TrafficTicketsIssuedETL.jar TrafficTicketsIssuedETL project/Traffic_Tickets_Issued.csv project/Tickets_ETL_final_output   -- to execute the traffic tickets ETL code


WEATHER DATA IN NYC	
	The data did not need ETL, profiling has been done in the next step.


4.	PROFILING

The codes related to profiling (including java files, class files and jar files) can be found in the /profiling_code folder. The files can be executed using the following commands:


PARKING VIOLATIONS
●	javac -classpath `yarn classpath` -d . ParkingViolationProfilingMapper.java  -- to compile the java file (class file already provided)
●	javac -classpath `yarn classpath`:. -d . ParkingViolationProfiling.java -- to compile the java file (class file already provided)
●	jar -cvf parkingProfile.jar ParkingViolationProf*.class  -- to create the jar file (file already provided)
●	hadoop jar parkingProfile.jar ParkingViolationProfiling project/Parking_Profile_final_output   -- to execute the parking violation ETL code

	MOTOR COLLISIONS
●	The cleaned data did not require profiling.


TRAFFIC TICKETS SUMMONSES
●	javac -classpath `yarn classpath` -d . TrafficTicketsIssuedProfilingMapper.java  -- to compile the java file (class file already provided)
●	javac -classpath `yarn classpath`:. -d . TrafficTicketsIssuedProfiling.java  -- to compile the java file (class file already provided)
●	jar -cvf TrafficTicketsIssuedProfiling.jar TrafficTickets*.class -- to create the jar file (file already provided)
●	hadoop jar TrafficTicketsIssuedProfiling.jar TrafficTicketsIssuedProfiling project/Tickets_ETL_final_output/part-r-00000 project/Tickets_Profile_output   -- to execute the traffic tickets Profiling code

		ITERATION 2:
●	javac -classpath `yarn classpath` -d . FinalTrafficTicketsIssuedMapper.java
-- to compile the java file (class file already provided)
●	javac -classpath `yarn classpath`:. -d . FinalTrafficTicketsIssued.java
		-- to compile the java file (class file already provided)
●	jar -cvf FinalTrafficTicketsIssued.jar FinalTrafficTickets*.class
		-- to create the jar file (file already provided)
●	hadoop jar FinalTrafficTicketsIssuedProfiling.jar FinalTrafficTicketsIssued project/Tickets_Profile_output/part-r-00000 project/Tickets_output_2
		-- to execute the traffic tickets Profiling code

		FINAL :
●	javac -classpath `yarn classpath`  -d . TrafficTicketsProfileFinalMapper.java 
-- to compile the java file (class file already provided)
●	javac -classpath `yarn classpath`:. -d . TrafficTicketsProfileFinal.java 
		-- to compile the java file (class file already provided)
●	jar -cvf TrafficTicketsProfileFinal.jar TrafficTicketsProfile*.class
		-- to create the jar file (file already provided)
●	hadoop jar TrafficTicketsProfileFinal.jar TrafficTicketsProfileFinal project/Tickets_output_2/part-r-00000 project/Tickets_Profile_final_output
		-- to execute the traffic tickets Profiling code

WEATHER DATA IN NYC	
●	javac -classpath `yarn classpath` -d . WeatherProfileMapper.java -- to compile the java file (class file already provided)
●	javac -classpath `yarn classpath`:. -d . WeatherProfile.java -- to compile the java file (class file already provided)
●	jar -cvf weatherProfile.jar WeatherProfile*.class -- to create the jar file (file already provided)
●	hadoop jar weatherProfile.jar WeatherProfile project/weather_data_nyc_centralpark_2016.csv project/Weather_Profile_final_output -- to execute the weather profile MapReduce job



5.	EXTRACTING OUTPUT FILES
	
The output files from ETL and Profiling stage needed to be put into the project packet, hence were extracted from HDFS using commands mentioned in ‘/data_extraction/extraction.sql’ file.
After having run these commands, the files can be brought into the local system using tools like FUGU or FileZilla. 
Also, the output files have been uploaded in the respective folders, 
E.g. output file for Parking Violations ETL is stored as -- /data_ETL/parkingETLOutput ,
   	     output file for Parking Violations Profile is stored as -- /data_ETL/parkingProfilingOutput , and so on.


6.	ANALYTICS

●	ANALYTIC 1: Correlation between petty offenses (parking violations and petty moving violations) and motor collisions
The code and outputs related to this file can be found in the folder “analytic/analytic1/code_an1.sql” and “analytic/analytic1/output” files respectively
The screenshots are present in the “/screenshots/analytic\ 1” folder.

●	ANALYTIC 2: Spotting a relation between changing weather and number of motor collisions
The code related to this file can be found in the folder  “analytic/analytic3/code_an3.sql” 

●	ANALYTIC 3: Identification of “hot-spots”, i.e. locations where collisions are a frequent phenomenon
		The code and output related to this file can be found in the folder  “analytic/analytic3/code_an3.sql” and “analytic/analytic3/output” files respectively. The visualization of the output is present in “analytic/analytic3/visualization/”

●	ANALYTIC 4: Spotting a relation between the days of week and number of motor collisions
The code and outputs related to this file can be found in the folder “analytic/analytic3/” and “analytic/analytic4/output” files respectively. The commands to run the code are mentioned in “Commands to Run the analytic.txt” file. The visualization of the output is present in “analytic/analytic4/visualization/”


●	ANALYTIC 5: Spotting a relation between offenses done by age groups.  The code and outputs related to this file can be found in the folder “analytic/analytic5/code_an5.sql” and “analytic/analytic5/output” files respectively. The screenshots are present in the “/screenshots/analytic\ 5” folder.
