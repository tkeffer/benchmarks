"""Time how long it takes to build a vector with the daily max temperature
for each day in a year, along with its time, using influxdb """
import sys
import os
import time

import influxdb
from weeutil.weeutil import timestamp_to_string
import weeutil.weeutil

from gen_fake_data import genFakeRecords

os.environ['TZ'] = 'America/Los_Angeles'

# Twelve months of data:
start_tt = (2010,1,1,0,0,0,0,0,-1)
stop_tt  = (2011,1,1,0,0,0,0,0,-1)
start_ts = int(time.mktime(start_tt))
stop_ts  = int(time.mktime(stop_tt))
interval = 300

giga = 1000000000

def main():
    client = influxdb.InfluxDBClient('localhost', 8086, 'root', 'root', 'example')
    
    # If there is no 'example' database, create it and fill it with fake data. 
    # Otherwise, do nothing
    try:
        client.create_database('example')
        gen_data(client)
    except influxdb.exceptions.InfluxDBClientError:
        pass

    # Now time a query:
    time_query(client)
        
def gen_data(client):
    """Function for generating fake data and putting it in the influxdb database."""
    
    # Create a giant JSON structure, then commit in one write. Much faster.
    giant_json = []
    N = 0
    for packet in genFakeRecords(start_ts=start_ts, stop_ts=stop_ts, interval=interval):
    
        print >> sys.stdout, "packets processed: %d; Last date: %s\r" % \
            (N, weeutil.weeutil.timestamp_to_string(packet["dateTime"])),
        sys.stdout.flush()
        
        json_body = {
            "measurement": "wxpacket",
            "tags": {
                "instrumentID": 1,
            },
            "time": packet["dateTime"] * 1000000000,  # Convert to nanoseconds
            "fields": {
                "interval" : packet["interval"],
                "usUnits"  : packet["usUnits"],
                "outTemp" : packet["outTemp"],
                "barometer" : packet["barometer"],
                "windSpeed" : packet["windSpeed"],
                "windDir"   : packet["windDir"],
                "windGust"  : packet["windSpeed"],
                "windGustDir" : packet["windDir"],
                "rain"      : packet["rain"]
            }
        }
        giant_json.append(json_body)
    
    # Do the write of the whole structure in a single commit:
    client.write_points(giant_json)

def time_query(client):
    """Time how long it takes to create a vector with daily max temperatures."""
    
    print "start time=", timestamp_to_string(start_ts)
    print "stop time=",  timestamp_to_string(stop_ts)
    print "(Approximately %.1f months of data)" % ((stop_ts - start_ts)/(30*24*3600))
    
    print "\nRunning a query for each day"
    t0 = time.time()
    vec = []
    
    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        query = 'SELECT MAX(outTemp) FROM "wxpacket" WHERE time > %d AND time <= %d' % (span[0] * giga, span[1] * giga)
        result_set = client.query(query, database='example')
        for x in result_set.get_points():
            vec.append(x["max"])

    t1 = time.time()
    print "Elapsed query time=", t1-t0
        
    print vec
    
    print "\nRunning a single query, with group by day"
    t0 = time.time()
    vec = []
    
    query = 'SELECT MAX(outTemp) FROM "wxpacket" WHERE time > %d AND time <= %d GROUP BY time(1d)' % (start_ts * giga, stop_ts * giga)
    result_set = client.query(query, database="example")
    for x in result_set.get_points():
        vec.append(x["max"])
    
    t1 = time.time()
    print "Elapsed query time=", t1-t0
    print vec
main()
