## Week 5 Homework 

Quick Reference:
- [General directions](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_5#general-directions)
- [Question 1](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_5#question-1)
- [Question 2](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_5#question-2)
- [Question 3](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_5#question-3)
- [Question 4](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_5#question-4)
- [Question 5](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_5#question-5)
- [Question 6](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/tree/main/week_5#question-6)

## General directions
In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHVHV 2021-06 data found here. [FHVHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz)


## Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
- 3.3.2
- 2.1.4
- 1.2.3
- 5.4

### Solution
For the installion process, refer [to this link](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing/setup).

For this question, we'll run `question_1.py`, you can check it [here](./question_1.py).

Output: `3.3.2`

Answer: `3.3.2`

## Question 2: 

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br> 
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>

- 2MB
- 24MB
- 100MB
- 250MB

### Solution

For this question, we'll run `question_2.py`, you can check it [here](./question_2.py).

After that, we'll run `ls -lha` in the `parquet/` path.

Output:
```bash
[...]
-rw-r--r-- 1 main main  25M Mar  9 16:30 part-00007-23fa6a18-7c97-4aaf-bc46-d30d8eecea90-c000.snappy.parquet
-rw-r--r-- 1 main main  25M Mar  9 16:30 part-00008-23fa6a18-7c97-4aaf-bc46-d30d8eecea90-c000.snappy.parquet
-rw-r--r-- 1 main main  25M Mar  9 16:30 part-00009-23fa6a18-7c97-4aaf-bc46-d30d8eecea90-c000.snappy.parquet
-rw-r--r-- 1 main main  25M Mar  9 16:30 part-00010-23fa6a18-7c97-4aaf-bc46-d30d8eecea90-c000.snappy.parquet
-rw-r--r-- 1 main main  25M Mar  9 16:30 part-00011-23fa6a18-7c97-4aaf-bc46-d30d8eecea90-c000.snappy.parquet
[...]
```

Answer: `24MB` (The closest one)

## Question 3: 

**Count records**  

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- 308,164
- 12,856
- 452,470
- 50,982

### Solution

For this question, we'll run `question_3.py`, you can check it [here](./question_3.py).

Output: 
```
+--------+
|count(*)|
+--------+
|  452470|
+--------+
```

Answer: `452,470`

## Question 4: 

**Longest trip for each day**  

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- 66.87 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours

### Solution

For this question, we'll run `question_4.py`, you can check it [here](./question_4.py).

Output:
```
+-------------------+-------------------+----------------+
|    pickup_datetime|   dropoff_datetime|   diff_in_hours|
+-------------------+-------------------+----------------+
|2021-06-25 13:55:41|2021-06-28 08:48:25|66.8788888888889|
+-------------------+-------------------+----------------+
```

Answer: `66.87 Hours`

## Question 5: 

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- 80
- 443
- 4040
- 8080

### Solution

We can access Spark UI via `port 4040`.

Answer: `4040`

## Question 6: 

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- East Chelsea
- Astoria
- Union Sq
- Crown Heights North

### Solution

For this question, we'll run `question_6.py`, you can check it [here](./question_6.py).

Output:
```
+-------------------+--------+
|               Zone|count(1)|
+-------------------+--------+
|Crown Heights North|  231279|
+-------------------+--------+
```

Answer: `Crown Heights North`
