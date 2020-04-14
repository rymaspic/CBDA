# NYC Mobility Analysis using Hadoop
By Xuankai Fang (xf569)

## 1. Original Data
1. Citi Bike Trip History Data Overview
- Data Source Link: [Citi Bike System Data | Citi Bike NYC](https://www.citibikenyc.com/system-data)
- Time: 2017-Nov (We use this month’s data for the test)
- Size: 238.1MB (1,330,651 columns, 15 rows)
- Format: CSV, to make consistent with my teammates, I re-saved it to TXT format.
- Original Data Scheme: Trip Duration (seconds), Start Time and Date, Stop Time and Date, Start Station Name, End Station Name, Station ID, Station Lat/Long, Bike ID, User Type (Customer = 24-hour pass or 3-day pass user; Subscriber = Annual Member), Gender (Zero=unknown; 1=male; 2=female), Year of Birth.
  Example:
  ![](citibike/F3E669EA-8C95-47BD-83B1-3106D97D596D.png)

## 2. Data Cleaning and Profiling
1. Data Profiling (codes are in DataClean.java): 
  As we discussed in our proposal, what we care about the traffic data are mainly two kinds of informations: **time period and location**. We want to track where do New Yorkers usually go at a certain time, to find some city motion and urban functional zone patterns of New York City. For example, at Friday night 9pm - 12pm, we find somewhere there are extremely high amount of traffic ending records, then this place might be somewhere famous for night life and offer relaxation for office workers.

So the process of the data profiling, that is, which columns to drop and which columns to save are accord with the above target.  Useless columns (though might be helpful if we go deeper in the future) like “gender”, “brith year”, “bikeid” and so on are all dropped.  And the “endtime”, “end station name”, “end station longitude and latitude” are kept. The process is realized by simply writing the strings we want as outputs in the Mapper class.

2. Data Clean(codes are in the same DataClean.java)
  For the data clean process, I mainly did:
  (1) Transform the [hour:minute:second] time to time period. What we really care is the volume of records in a certain time period, so I need to change the exact time to a time period class. Now I use the 1-hour as the interval that is finally I have 0-23 to denotes the time period class. Later, I could change the time interval according to the analysis we are going to perform.
  **11:20:09 -> 11**
  (2) Generate time day name of the week from the [year-month-day] data. Since the original data do not have the day name data (Monday, Tuesday, …) which we need for the analysis. So here I extract the day name of week information from the date data.
  **2017-11-01 -> 2017-11-01, 1 (1-7 represents Monday to Sunday)**
  (3) Count the record in the Reducer.
  In the Mapper I generate a series of (NewScheme, 1)…., and for the analysis, we also want to know the volume data, so I need to count the record belonging to the same group (the same time period and the same location).
3. Result:
  (1) an example of the test data
  I started from a simple test file, that is, to finish a “toy job” first. 
  ![](citibike/%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202019-11-04%20%E4%B8%8A%E5%8D%8810.46.42.png)
  Final output:
  ![](citibike/EF9A1E6E-5594-4BF8-AED4-C880430E80AD.png)
  We can see the MapReduce job successfully finished the data cleaning and profiling job and count the right number of the same group.
  (2) the final output of the whole data (I can just show you a part of them)
  ![](citibike/DEF1667C-13EE-44CE-83E9-FC3CA518D263.png)
  In Thursday 10AM, the location of “6 Ave & Canal St” has an extremely high volume of ending record of citibike use. I guess this might be “Wall Street Bankers” who wants to anti the “Climate Change” so they prefer to ride a bike to go to work:). This is just a naive example for finding the “patterns”, we will later do more complex analysis combining other traffic data like MTA and normal vehicle as done by my teammates.

## 3. Data Scheme
Just like what showed in the above MapReduce job result screenshots, the final scheme of my data are:       
![](citibike/B206E591-08F4-4F6B-B1BF-B6AC364E9104.png)
Note: The output of the MapReduce format is {Key: Text (Scheme), Value: IntWritable(Volume)}. 

- Year-MM-DD: the date of the record, so far it ranges from 2017-11-01 to 2017-11-30
- WeekName: the day name of the week, (1-7), 1 represents Monday…
- TimePeriod: the time period of a day, (0-24), 0 represents the record happened in 00:00:00 to 00:59:59.
- StationName: the name of the citibike station data, reflecting the approximate direct location information.
- StationLonLat: the geometry coordinations of the station, reflecting the exact location.
- Volume: the counted number of the record belonging to the same scheme.


## 4. Data Analysis: Count the average
The codes are in Average.java.

1. Run the code on the HDFS
```
javac -classpath `yarn classpath` -d . Average.java
jar -cvf Average.jar *.class
hadoop jar Average.jar Average /user/xf569/CBDA/test-2.txt /user/xf569/CBDA/test2-output
```

![](citibike/E55B46AE-3AE4-4590-A896-853FD1C62981.png)
![](citibike/CFA63F9F-C5C2-4602-AA80-8A4B36D129B1.png)
Change the separator here from “/t” to “,” for the convenience of later analysis in Hive. (in java MapReduce code set the configuration.)
![](citibike/A2D98E1E-AF9A-40E2-B711-828FE83623C4.png)

`hadoop jar Average.jar Average /user/xf569/CBDA/dataclean-output/part-r-00000 /user/xf569/CBDA/average-output`

`hdfs dfs -cat /user/xf569/CBDA/average-output/part-r-00000`

![](citibike/4FF3B282-0C4E-44BC-BDE3-BD747F5D007B.png)


## 5. Data Analysis: Query Using the Hive
### Create Tables
1. First, create the Hive table using the output txt file.
```
create external table test2 (weekName string,timePeriod int,address string,lon double,lat double,avgVolume double) row format delimited fields terminated by ',' location '/user/xf569/CBDA/test2-output/part-r-00000';
```

2. Some trial commands and basic analysis:

`show tables;`

![](citibike/314431FD-20FC-4231-B16F-A61451C51114.png)

`describe test2;`

![](citibike/FD27DA9A-AF8E-4E22-85FE-6BCD74558E24.png)

`select * from test2;`

![](citibike/29DBC0B3-2A3D-4D8A-B078-8329CD058B78.png)

`select * from test2 where weekname = "2" and timeperiod = 10;`

![](citibike/AA8A9851-D05E-4D0B-AA7D-0A53DE9EA5DE.png)

3. Create the table using the whole data
  In Hive command environment:
```
create external table average1 (weekName string,timePeriod int,address string,lon double,lat double,avgVolume double) row format delimited fields terminated by ',' location '/user/xf569/CBDA/average-output/';
```

![](citibike/95FC2022-8628-47EB-880C-E926520FFEDF.png)

`select * from average1 limit 20;`

![](citibike/DB31DFF5-A7AA-4069-8204-2FB00AD5D134.png)

###  Hive analysis for our project
1. Track the Commuting Time: get insights of NYC office area.
  Sample: Mean Value of Weekdays (Mon-Fri) From 7 AM-9 AM:
  HiveQL:
```sql
FROM average1 SELECT address,lon,lat,(sum(avgvolume)/5.0) AS meanAvg WHERE ((weekname != "6" and weekname != "7") and (timeperiod = 7 or timeperiod = 8)) GROUP BY address,lon,lat SORT by meanAvg desc limit 10;
```
It might take a long time to run.
Screenshot of the result:
![](citibike/B2A7D300-A671-4F54-BF20-881B239E9C69.png)

2. Track the Friday night: get insights of Nightlife/Entertainment are in NYC. 
  Sample: Friday Night 8PM-9PM

```sql
select * from average1 where (weekname = "5" and timeperiod = 20) sort by avgvolume desc limit 10;
```

![](citibike/339DD294-DD97-4C65-8F79-0252CE0BB125.png)

3. Track the Sunday afternoon: get insights of where do New Yorkers spend the weekend afternoon.
  Sample: Sunday 2PM-3PM

```sql
select * from average1 where (weekname = "7" and timeperiod = 14) sort by avgvolume desc limit 10;
```

![](citibike/84B6D210-19BA-4B57-9B74-E3D1ACCF6975.png)

4. Track the volume changing trends on a stable location on Monday (choose the place we find has most records on commuting time in step 1)

```sql
select * from average1 where (weekname = "1" and address = "Pershing Square North") order by cast(timeperiod as int) asc;
```

![](citibike/D7CBEF2E-0DD0-41B9-B9B3-62D62EFDD987.png)

5. Track the volume changing trends on a stable location on the mean of all weekdays

```sql
FROM average1 SELECT address,lon,lat,timeperiod,(sum(avgvolume)/5.0) AS meanAvg WHERE ((weekname != "6" and weekname != "7") and address = "Pershing Square North") GROUP BY address,lon,lat,timeperiod ORDER by cast(timeperiod as int) asc
```

![](citibike/69CBF536-CFE6-4710-BD6E-7231275A8104.png)




