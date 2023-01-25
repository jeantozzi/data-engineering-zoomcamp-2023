## Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL

Quick Reference:
- [Question 1 - Knowing docker tags](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/edit/main/week_1/README.md#question-1-knowing-docker-tags)
- [Question 2 - Understanding docker first run](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/edit/main/week_1/README.md#question-2-understanding-docker-first-run)
- [Question 3 - Count records](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/edit/main/week_1/README.md#question-3-count-records)
- [Question 4 - Largest trip for each day](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/edit/main/week_1/README.md#question-4-largest-trip-for-each-day)
- [Question 5 - The number of passengers](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/edit/main/week_1/README.md#question-5-the-number-of-passengers)
- [Question 6 - Largest tip](https://github.com/jeantozzi/data-engineering-zoomcamp-2023/edit/main/week_1/README.md#question-6-largest-tip)

## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string`
- `--idimage string`
- `--idfile string`

## Solution

Command:
```
docker build --help | grep ID
```

Output:
```
      --iidfile string          Write the image ID to the file
  -q, --quiet                   Suppress the build output and print image ID on success
```

Answer:
`--iidfile string`

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- 3
- 7

## Solution

Command:
```
docker run -it --entrypoint=/bin/bash python:3.9
pip list
```

Output:
```
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
```

Answer:
`3`

## Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

## Solution

We'll use `docker-compose.yaml` and `ingest_data_homework` for this setup.

### Step 1
We have to build the python ingestor image, so it will be available for us to run the container.
Run `docker build -t ingestor .` while in the same folder as the `Dockerfile` file.

### Step 2
`docker-compose.yaml` will be used to build two containers, `pgdatabase` (Postgres RDBMS) and `pgadmin` (Postgres GUI Client).
You only need to run `docker-compose up -d` while in the same folder as the `docker-compose.yaml` file.

### Step 3
Now that the RDBMS and the GUI Client are running, we can execute run the python ingestor container. It will download the taxi data based on the URL and ingest into Postgres.
Run the following commands:
```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

docker run -it \
  --network=pg \
  ingestor \
  --user=root \
  --password=root \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi \
  --table_name=taxi_trips \
  --url=${URL}
```

### Step 4
Connect to PGAdmin via `http://localhost:8080/`, login `admin@admin.com`, password `root`. Register a new server by filling Host name/address `localhost`, username `root` and password `root`.
You are now good to query the data and answer the questions below.

## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- 20530
- 17630
- 21090

## Solution

Command:
```
SELECT COUNT(1)
FROM taxi_trips
WHERE DATE_TRUNC('DAY', lpep_pickup_datetime) = CAST('2019-01-15' AS timestamp)
  AND DATE_TRUNC('DAY', lpep_dropoff_datetime) = CAST('2019-01-15' AS timestamp)
```

Output:
```
20530
```

Answer:
`20530`

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- 2019-01-15
- 2019-01-10

## Solution

Command:
```
SELECT CAST(lpep_pickup_datetime AS date)
FROM taxi_trips
ORDER BY trip_distance DESC
LIMIT 1
```

Output:
```
2019-01-15
```

Answer:
`2019-01-15`

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254
- 2: 1282 ; 3: 274

## Solution

Command:
```
SELECT COUNT(1)
FROM taxi_trips
WHERE DATE_TRUNC('DAY', lpep_pickup_datetime) = CAST('2019-01-01' AS timestamp)
	AND passenger_count = 2;
 
SELECT COUNT(1)
FROM taxi_trips
WHERE DATE_TRUNC('DAY', lpep_pickup_datetime) = CAST('2019-01-01' AS timestamp)
	AND passenger_count = 3;
```

Output:
```
1282
254
```

Answer:
`2: 1282 ; 3: 254`

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- Long Island City/Queens Plaza

## Solution

Command:
```
SELECT zdo."Zone"
FROM taxi_trips AS t
	JOIN zones AS zpu
		ON t."PULocationID" = zpu."LocationID"
	JOIN zones AS zdo
		ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'Astoria'
ORDER BY t."tip_amount" DESC
LIMIT 1
```

Output:
```
Long Island City/Queens Plaza
```

Answer:
`Long Island City/Queens Plaza`
