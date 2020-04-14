# NYC CitiBike Mobility Analysis using Hadoop MapReduce and Hive
This GitHub project is a simple data cleaning and analysis program to get some basic insights from the raw CitiBike Trip History Data. By implementing similar program over the CitiBike dataset, you could get a new dataset containing volume information of the CitiBike records at a certain time (eg. Monday 8-9AM) as well as at a certain place (eg. Wall Street). Then you could use the Hive or any other data analysis SQL-tools to get more insights based on the rules you define. I demoed a simple application to find ranked places at certain time to research urban functional zones  at NYC area.

## 1. Original Data

1. Citi Bike Trip History Data Overview
- Data Source Link: [Citi Bike System Data | Citi Bike NYC](https://www.citibikenyc.com/system-data)
- Time: 2017-Nov (We use this month’s data for the test)
- Size: 238.1MB (1,330,651 columns, 15 rows)
- Format: CSV, to make consistent with my teammates, I re-saved it to TXT format.
- Original Data Scheme: Trip Duration (seconds), Start Time and Date, Stop Time and Date, Start Station Name, End Station Name, Station ID, Station Lat/Long, Bike ID, User Type (Customer = 24-hour pass or 3-day pass user; Subscriber = Annual Member), Gender (Zero=unknown; 1=male; 2=female), Year of Birth.
  Example:
  ![](https://github.com/rymaspic/CBDA/blob/master/imgs/1-1.png)

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
  ![](https://github.com/rymaspic/CBDA/blob/master/imgs/3-1.png)

  Final output:
  ![](https://github.com/rymaspic/CBDA/blob/master/imgs/3-2.png)
  We can see the MapReduce job successfully finished the data cleaning and profiling job and count the right number of the same group.
  (2) the final output of the whole data (I can just show you a part of them)
  ![](https://github.com/rymaspic/CBDA/blob/master/imgs/3-3.png)
  In Thursday 10AM, the location of “6 Ave & Canal St” has an extremely high volume of ending record of citibike use. I guess this might be “Wall Street Bankers” who wants to anti the “Climate Change” so they prefer to ride a bike to go to work:). This is just a naive example for finding the “patterns”, we will later do more complex analysis combining other traffic data like MTA and normal vehicle as done by my teammates.

## 3. Data Scheme
Just like what showed in the above MapReduce job result screenshots, the final scheme of my data are:       
![](https://github.com/rymaspic/CBDA/blob/master/imgs/4-1.png)
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

![](https://github.com/rymaspic/CBDA/blob/master/imgs/4-2.png)
![](https://github.com/rymaspic/CBDA/blob/master/imgs/4-3.png)
Change the separator here from “/t” to “,” for the convenience of later analysis in Hive. (in java MapReduce code set the configuration.)
![](https://github.com/rymaspic/CBDA/blob/master/imgs/4-4.png)

`hadoop jar Average.jar Average /user/xf569/CBDA/dataclean-output/part-r-00000 /user/xf569/CBDA/average-output`

`hdfs dfs -cat /user/xf569/CBDA/average-output/part-r-00000`

![](https://github.com/rymaspic/CBDA/blob/master/imgs/4-5.png)


## 5. Data Analysis: Query Using the Hive
Details in [Hive Scripts and Result-Screenshot.pdf](https://github.com/rymaspic/CBDA/blob/master/src/Hive%20Scripts%20and%20Result-Screenshot.pdf).

We also have applied similar procedures on the NYC MTA subway data and NYC Traffic Volume data to get some interesting conclusions. If you want to know about facts like "where do most New Yorkers spend their night life?" "where are some hot areas for New Yorkers go to work", please refer to our [report paper](https://drive.google.com/file/d/1YKBwHI4x3gs3o6aNar0XH2W5DS7uxfiO/view?usp=sharing).






