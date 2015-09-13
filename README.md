# benchmarks
Benchmarks for comparing sqlite, MySQL, MongoDB, and InfluxDB for a common
task when doing weather data analysis: create an array holding
the max daily temperature and the time it was achieved, for 
each day in a year.

Uses the module `gen_fake_data` from the weewx test suites to create a year's worth of synthetic data,
with a 5 minute archive interval. This is approximately 105,000 records.

Both the weewx test suites and the weewx API must be in your `PYTHONPATH`. The following works for me:

```
export PYTHONPATH="/home/tkeffer/git/weewx/bin:/home/tkeffer/git/weewx/bin/weewx/test"
```

## Results
Using an Intel NUC.


### SQLITE

Query strategies:

1. Using an explicit SQL query for each day of the year

    ```
    SELECT dateTime, outTemp FROM archive WHERE dateTime > %d AND dateTime <= %d AND 
        outTemp = (SELECT MAX(outTemp) FROM archive WHERE dateTime > %d AND dateTime <= %d)
    ```

    where the `%d` is filled in by the start and stop of each day in local time. 

2. Using the weewx `manager.getAggregate()` function. This actually involves two queries: one for the max temperature, 
the other for the time of the max temperature. 

3. Using the weewx daily `manager.getAggregate()` function. This takes advantage of the daily summaries
built by the weewx class `WXDaySummaryManager`.

On `/var/tmp` (a conventional HD):

1. 0.106

2. 0.140

3. 0.055

On `/tmp` (an SSD)

1. 0.075

2. 0.127

3. 0.054


### MySQL

Using the same three query strategies as used above with sqlite.

First run

1. 0.16

2. 0.27

3. 0.18

Second run (presumably after caches have been filled):

1. 0.026

2. 0.048

3. 0.068


### [MongoDB](https://www.mongodb.com/)

It's worth noting that inserting the 100,000+ records in the MongoDB database
was very fast, approximately 25 seconds, even though they were inserted individually.
Presumably, a bulk insert would be even faster.

The collection was indexed on the timestamp `dateTime`.

The query strategy is to do an aggregation for each day of the year (local time).

    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        rs = collection.aggregate([{"$match"   : {"dateTime" : {"$gt" : datetime.datetime.utcfromtimestamp(span[0]), 
                                                                "$lte" : datetime.datetime.utcfromtimestamp(span[1])}}}, 
                                   {"$project" : {"dateTime" : 1, "outTemp" : 1}},
                                   {"$sort" : {"outTemp":-1 } },
                                   {"$limit" : 1}
                                   ])

Total run time for the query was approximately 0.3s.


### [InfluxDB](https://influxdb.com/)

The query

```
SELECT MAX(outTemp) FROM "wxpacket" WHERE time > %d AND time <=%d
```

gets the max temperature of each day and took 1.37 seconds. I couldn't find a way to get
the time of each max in the same query (InfluxDB does not allow a SELECT of a SELECT, like SQL).

Using the `GROUP BY` query:

```
SELECT MAX(outTemp) FROM "wxpacket" WHERE time > %d AND time <= %d GROUP BY time(1d)
```

took 0.47 seconds. Unfortunately, it's giving the wrong answer because the days
are being grouped by UTC time, not local time. It doesn't return the time of the max either.
