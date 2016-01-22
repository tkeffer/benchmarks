"""Time how long it takes to build a vector holding the daily max temperature,
along with the time it was achieved, for each day in a year, 
using a Sixth Normal Form schema.

This benchmark runs the test on both sqlite and MySQL, using the weedb library."""
import sys
import os
import time

import weedb

from weeutil.weeutil import timestamp_to_string
import weeutil.weeutil

from gen_fake_data import genFakeRecords

os.environ['TZ'] = 'America/Los_Angeles'

sqlite_db_dict = {'database_name': '/var/tmp/bench.sdb', 'driver':'weedb.sqlite'}
mysql_db_dict  = {'database_name': 'benchmark', 'user':'weewx', 'password':'weewx', 'driver':'weedb.mysql'}

# Twelve months of data:
start_tt = (2010,1,1,0,0,0,0,0,-1)
stop_tt  = (2011,1,1,0,0,0,0,0,-1)
start_ts = int(time.mktime(start_tt))
stop_ts  = int(time.mktime(stop_tt))
interval = 300

def main():

    print "start time=", timestamp_to_string(start_ts)
    print "stop time= ",  timestamp_to_string(stop_ts)
    print "(Approximately %.1f days of data)" % ((stop_ts - start_ts)/(24*3600.0))
     
    print "***** SQLITE *****"
    create_table(sqlite_db_dict)
    connect = weedb.connect(sqlite_db_dict)
    time_query(connect, 'outTemp')
    time_query(connect, 'barometer')
    connect.close()
    
    print "***** MySQL *****"
    create_table(mysql_db_dict)
    connect = weedb.connect(mysql_db_dict)
    time_query(connect, 'outTemp')
    time_query(connect, 'barometer')
    connect.close()

def create_table(db_dict):
    """Create and populate the database table using a 6NF schema"""
    
    # If the following is uncommented, the data will be deleted
    # before every run.
#     try:
#         weedb.drop(db_dict)
#     except weedb.NoDatabase:
#         pass
    
    # Try to create the database. If it already exists,
    # an exception will be raised. Be prepared to catch it
    try:
        weedb.create(db_dict)
    except weedb.DatabaseExists:
        pass
    
    connect = weedb.connect(db_dict)
    cursor = connect.cursor()

    # Create the table and generate the data. If it already exists,
    # an exception will be raised. Be prepared to catch it 
    # and skip generating the data.
    try:
        # Note that every measurement gets its own row
        # The primary key is the combination of the timestamp and observation type
        cursor.execute("CREATE TABLE bench ("
                       "dateTime REAL NOT NULL, "
                       "obstype VARCHAR(63) NOT NULL, "
                       "measurement REAL, "
                       "CONSTRAINT pk PRIMARY KEY (dateTime, obstype))")
    except weedb.OperationalError:
        print "Benchmark data already exists"
    else:
        print "Generating fake data"
        gen_data(connect)
    finally:
        cursor.close()
        connect.close()
         
def gen_data(connect):
    """Function for generating fake data and putting it in the database in 6NF."""
 
    t0 = time.time()

    cursor = connect.cursor()
    connect.begin()    

    N = 0
    for packet in genFakeRecords(start_ts=start_ts, stop_ts=stop_ts, interval=interval):
     
        # Break the packet into separate observation type
        for obstype in ['outTemp', 'barometer', 'windSpeed', 'windDir', 'windGust', 'windGustDir', 'rain']:
            # Put each observation type in its own row
            cursor.execute("INSERT INTO bench VALUES (?, ?, ?);", (packet['dateTime'], obstype, packet[obstype]))
            N += 1
            # Commit every 32,0000 inserts.
            if (N % 32000) == 0 :
                connect.commit()
                connect.begin()
                print >> sys.stdout, "packets processed: %d; Last date: %s\r" % \
                    (N, weeutil.weeutil.timestamp_to_string(packet["dateTime"])),
                sys.stdout.flush()
 
    connect.commit()
    cursor.close()
    t1 = time.time()
    print "\n%d records generated in %.1f seconds" % (N, t1-t0)
 
def time_query(connect, obs_type):
    """Time how long it takes to create a vector with daily max temperatures."""
     
    t0 = time.time()
    cursor = connect.cursor()
    vec = []
     
    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        query = "SELECT dateTime, measurement FROM bench WHERE obstype = '%s' AND dateTime > ? AND dateTime <= ? AND " \
                    "measurement = (SELECT MAX(measurement) FROM bench " \
                    "WHERE obstype = '%s' AND dateTime > ? AND dateTime <= ?)" % (obs_type, obs_type)
        cursor.execute(query, span + span)
        result_set = cursor.fetchone()
        vec.append(tuple(result_set))
 
    t1 = time.time()
    cursor.close()
    print "For observation type %s, elapsed query time = %.03f" % (obs_type, t1-t0)

main()
