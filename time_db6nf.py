"""Time how long it takes to build a vector with the daily max temperature
for each day in a year, along with its time, 
using a MySQL schema in Sixth Normal Form"""
import sys
import os
import time

import MySQLdb
import _mysql_exceptions

from weeutil.weeutil import timestamp_to_string
import weeutil.weeutil

from gen_fake_data import genFakeRecords

os.environ['TZ'] = 'America/Los_Angeles'

host='localhost'
user = 'weewx'
password = 'weewx'
database_name = 'benchmark'


# Twelve months of data:
start_tt = (2010,1,1,0,0,0,0,0,-1)
stop_tt  = (2011,1,1,0,0,0,0,0,-1)
start_ts = int(time.mktime(start_tt))
stop_ts  = int(time.mktime(stop_tt))
interval = 300

def main():
    connect = MySQLdb.connect(host=host, user=user, passwd=password)

    cursor = connect.cursor()
    try:
        # Try to create the database. It may already exist. 
        # In that case be prepared to catch the exception
        cursor.execute("CREATE DATABASE %s" % database_name)
    except _mysql_exceptions.ProgrammingError:
        pass

    cursor.execute("USE %s" % database_name)

    # This may be useful while testing
#     try:
#         cursor.execute("DROP TABLE bench")
#     except _mysql_exceptions.OperationalError:
#         pass
    
    # Create the table and generate the data.
    # It may already exist, so be prepared to catch the exception
    # and skip generating the data.
    try:
        # Note that every measurement gets its own row
        # The primary key is the combination of the observation type and a timestamp
        cursor.execute("CREATE TABLE bench ("
                       "obstype VARCHAR(63) NOT NULL, "
                       "dateTime REAL NOT NULL, "
                       "measurement REAL, "
                       "CONSTRAINT pk PRIMARY KEY (obstype, dateTime))")
    except _mysql_exceptions.OperationalError:
        print "Benchmark data already exists"
    else:
        gen_data(connect)
    finally:
        cursor.close()
    
    # Now time a query:
    time_query(connect)
        
def gen_data(connect):
    """Function for generating fake data and putting it in the MySQL database in 6NF."""

    t1 = time.time()
    cursor = connect.cursor()    
    connect.query("START TRANSACTION")
    N = 0
    for packet in genFakeRecords(start_ts=start_ts, stop_ts=stop_ts, interval=interval):
    
        # Break the packet into separate observation type
        for obstype in ['outTemp', 'barometer', 'windSpeed', 'windDir', 'windGust', 'windGustDir', 'rain']:
            # Put each observation type in its own row
            cursor.execute("INSERT INTO bench VALUES (%s, %s, %s);", (obstype, packet['dateTime'], packet[obstype]))
            N += 1
            # Commit every 1000 inserts.
            if (N % 1000) == 0 :
                connect.commit()
                print >> sys.stdout, "packets processed: %d; Last date: %s\r" % \
                    (N, weeutil.weeutil.timestamp_to_string(packet["dateTime"])),
                sys.stdout.flush()

    connect.commit()
    cursor.close()
    t2 = time.time()
    print "\n%d records generated in %.1f seconds" % (N, t2-t1)

def time_query(connect):
    """Time how long it takes to create a vector with daily max temperatures."""
    
    print "start time=", timestamp_to_string(start_ts)
    print "stop time=",  timestamp_to_string(stop_ts)
    print "(Approximately %.1f months of data)" % ((stop_ts - start_ts)/(30*24*3600))
    
    print "\nRunning a query for each day"
    t0 = time.time()
    cursor = connect.cursor()
    vec = []
    
    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        query = "SELECT dateTime, measurement FROM bench WHERE obstype = 'outTEMP' AND dateTime > %s AND dateTime <= %s AND " \
                    "measurement = (SELECT MAX(measurement) FROM bench WHERE obstype = 'outTemp' AND dateTime > %s AND dateTime <= %s)"
        cursor.execute(query, span + span)
        result_set = cursor.fetchone()
        vec.append(result_set)

    t1 = time.time()
    print "Elapsed query time=", t1-t0
        
    print vec
    
main()
