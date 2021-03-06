# benchmarks
Benchmarks for comparing sqlite, MySQL, MongoDB, and InfluxDB for a common
task when doing weather data analysis: create an array holding
the max daily temperature and the time it was achieved, for 
each day of a year.

Uses the module `gen_fake_data` from the weewx test suites to create a year's worth of synthetic data,
with a 5 minute archive interval. This is approximately 105,000 records.

Both the weewx test suites and the weewx API must be in your `PYTHONPATH`. The following works for me:

```
export PYTHONPATH="/home/tkeffer/git/weewx/bin:/home/tkeffer/git/weewx/bin/weewx/test"
```

## Results
Using an Intel NUC with Intel® Core™ i5-4250U CPU @ 1.30GHz × 4, and 16 GB of memory.


### SQLITE

It took approximately 4 seconds to create the archive database as a single transaction, 
and another 18.25s to create the daily summaries.

The table was indexed on the timestamp `dateTime`.

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

On `/var/tmp`, a conventional hard drive:

1. 0.106

2. 0.140

3. 0.055

On `/tmp`, a solid-state disk:

1. 0.075

2. 0.127

3. 0.054


### MySQL

The MySQL database was on `/var/lib`, a conventional hard drive.

It took 15.4s to create the archive database as a single transaction, and another 25.6s to create the daily
summaries.

The table was indexed on the timestamp `dateTime`.

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

It's worth noting that even though the 100,000+ records were inserted individually into the 
MongoDB database, it only took 25 seconds. While a bulk insert would be presumably faster,
this was fast enough, so I didn't bother.

The collection was indexed on the timestamp `dateTime`.

The query strategy is similar to strategy #1 above, used with the SQL databases: do an aggregation
for each day of the year (local time).

    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        rs = collection.aggregate([{"$match"   : {"dateTime" : {"$gt"  : datetime.datetime.utcfromtimestamp(span[0]), 
                                                                "$lte" : datetime.datetime.utcfromtimestamp(span[1])},
                                                  "outTemp"  : {"$ne" : None}}}, 
                                   {"$project" : {"dateTime" : 1, "outTemp" : 1}},
                                   {"$sort" : {"outTemp":-1 } },
                                   {"$limit" : 1}
                                   ])

The match for non-null values of `outTemp` is not really necessary when you are looking
for the largest value of something, but it is when you are looking for the smallest, because [null sorts
before numbers](http://docs.mongodb.org/master/faq/developers/#what-is-the-compare-order-for-bson-types)
in MongoDB.

Total run time for the query was approximately 0.35s.


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

## Additional tests using a Sixth Normal Form schema

Later, I did some additional test using a Sixth Normal Form (6NF) schema. In 6NF, each row consists of just
a key and one value. In this context, this means each row can hold only a single measurement value.

The table looked like this

| dateTime   | obstype     | measurement |
|------------|-------------|-------------|
| 1262332800 | `outTemp`   | 20.5        |
| 1262332800 | `windSpeed` | 12.5        |
| 1262332800 | `windDir`   | 230.0       |
| 1262484000 | `outTemp`   | 20.6        |
| 1262484000 | `windSpeed` | 9.5         |
| 1262484000 | `windDir`   | 235.0       |
| ...        | ...         | ...         |

The primary key is the combination (`obstype`, `dateTime`).

The query looked like this:

```Python
    vec = []
    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        query = "SELECT dateTime, measurement FROM bench WHERE obstype = 'outTemp' AND dateTime > ? AND dateTime <= ? AND " \
                    "measurement = (SELECT MAX(measurement) FROM bench WHERE obstype = 'outTemp' AND dateTime > ? AND dateTime <= ?)"
        cursor.execute(query, span + span)
        vec.append(tuple(cursor.fetchone()))
```
#### SQLITE using 6NF

Using `/var/tmp`, mounted on a conventional hard drive.

| Test                                                   | time  |
|--------------------------------------------------------|-------|
| Generate data in a single transaction                  | 5.2s  |
| Generate data in transaction batches of 32,000 records | 8.3s  |
| Run the query, first run                               | 0.48s |
| Run the query, subsequent runs                         | 0.48s |


#### MySQL using 6NF

Run on a conventional hard drive.

| Test                                                   | time   |
|--------------------------------------------------------|--------|
| Generate data in a single transaction                  | 80s    |
| Generate data in transaction batches of 32,000 records | 78s    |
| Run the query, first run                               | 0.47s  |
| Run the query, subsequent runs                         | 0.021s |

Clearly, filling the caches makes a big difference for MySQL

#### Conclusion

Because the observation type is specified as a key, and not
a schema attribute, 6NF has the advantage of flexibility. New types can be added easily.

Unfortunately, queries seem to take about 3 times as long as the standard schema.

It's worth noting that MongoDB also allows any mix of types, but takes about twice as long as MySQL using the
standard schema.
