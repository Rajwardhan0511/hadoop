Author: Rajwardhan Shinde

Environment: Big Data VM
Created on: 25/11/2024

`````````````
Pre-requisite:
start these-
1) HDFS		
	hdfs namenode -format
	./run-hdfs.sh -s start
	
2) YARN	
	./run-yarn.sh -s start
	hdfs dfs -mkdir -p /user/talentum

3) HIVE
	./run-hivemetastore.sh -s start
	./run-hiveserver2.sh -s start

``````
Usage: 
bash workflow.sh

`````````````````````````````````````````````

1) create table Startups Look at the data and decide the columns along with their data types
2) Use LazySimpleSerde for table creation
3) LOAD DATA from the file Listofstartups.hive
4) Make sure that the header line won't be the part of loaded data
5) Verify that data loaded properly

6) Any issues after using above serde? Note down the issues in your answer
"""
	Fields containing commas within quotes break the parsing since LazySimpleSerDe does not support quoted field delimiters. As a result, the data is not distributed correctly across columns, leading to incorrect query results.
"""

7) Find a right serde for this data (You can google it)
"""
OpenCSVSerDe:
	It is a Hive SerDe used for parsing CSV files, particularly those containing quoted fields with commas. It handles complex CSV structures, supports custom delimiters, and can skip header rows during data load. This makes it ideal for CSV files with embedded commas or other special characters, ensuring accurate data parsing.
"""

8) Find which sector has the most startups?
	select sector, COUNT(*) as count
	from startups
	group by sector
	order by count desc
	limit 1;
"""
	This query counts the number of startups in each sector, groups the results by sector, orders them in descending order by the count, and returns the sector with the highest number of startups.
"""

9) Find which State has the max number of startups?
	select coalesce(trim(split(location_of_company,',')[1]),location_of_company), 
		count(*) as count
	from startups
	group by coalesce(trim(split(location_of_company,',')[1]),location_of_company)
	order by count desc
	limit 1;
"""
	This query extracts the state from the location_of_company field by splitting it at the comma, trims any extra spaces, and counts the number of startups in each state. It then returns the state with the highest number of startups.
"""

10) Find the total startups from “Maharashtra”?
	select count(*) as count
	from startups
	where location_of_company RLIKE "Maharashtra";
"""
	This query counts the number of startups located in Maharashtra by checking if the location_of_company field contains the word "Maharashtra" using a regular expression.
"""

11) How many startups were formed in “Healthcare”
	select count(*) as count
	from startups
	where sector RLIKE "Healthcare";
"""
	This query counts the number of startups in the "Healthcare" sector by matching the sector field with the regular expression "Healthcare".
"""
