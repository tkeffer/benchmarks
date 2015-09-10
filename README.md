# benchmarks
The goal is to create an array holding the max daily temperature and the time it was achieved, for 
each day in a year. Should be 366 points of data.

Uses the module `gen_fake_data` from the weewx test suites to create the synthetic data.

So, both the test suites and the weewx API must be in your PATH. The following works for me:

```
export PYTHONPATH="/home/tkeffer/git/weewx/bin:/home/tkeffer/git/weewx/bin/weewx/test"
```

## Results
Using an Intel NUC, with a full year of synthetic data (using gen_fake_data()) at 5 minute intervals,
a total of about 105,000 data points.


### SQLITE

Query strategies:

1. Using an explicit query:

    ```
    SELECT dateTime, outTemp FROM archive WHERE dateTime > %d AND dateTime <= %d AND 
        outTemp = (SELECT MAX(outTemp) FROM archive WHERE dateTime > %d AND dateTime <= %d)
    ```

    where the `%d` is filled in by the start and stop of each day in local time. 

2. Using the weewx `manager.getAggregate()` function. This actually involves two queries: one for the max temperature, 
the other for the time of the max temperature. 

3. Using the weewx daily `manager.getAggregate()` function.

On `/var/tmp` (a conventional HD):

1. 0.106

2. 0.140

3. 0.055

On `/tmp` (an SSD)

1. 0.075

2. 0.127

3. 0.054


### MySQL

Same query strategy as sqlite.

First run

1. 0.16

2. 0.27

3. 0.18

Second run (presumably after caches have been filled):

1. 0.026

2. 0.048

3. 0.068


### [InfluxDB](https://influxdb.com/)

I couldn't figure out a way to get the time of the max temps. Used the query

```
SELECT MAX(outTemp) FROM "wxpacket" WHERE time > %d AND time <=%d
```

which took 1.37 seconds. 

Using the `GROUP BY` query:

```
SELECT MAX(outTemp) FROM "wxpacket" WHERE time > %d AND time <= %d GROUP BY time(1d)
```

took 0.47 seconds. Better, but not in the same league as sqlite and MySQL. Unfortunately, it gives the wrong 
answer around DST boundaries. It also doesn't return the time of max temperature.
