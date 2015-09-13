"""Time how long it takes to build a vector with the daily max temperature
for each day in a year, along with its time, using sqlite and MySQL """
import os
import time
import StringIO

import configobj

import weewx.manager
import gen_fake_data    # From the weewx test suites

from weeutil.weeutil import timestamp_to_string
import weeutil.weeutil

os.environ['TZ'] = 'America/Los_Angeles'

# Twelve months of data:
start_tt = (2010,1,1,0,0,0,0,0,-1)
stop_tt  = (2011,1,1,0,0,0,0,0,-1)
start_ts = int(time.mktime(start_tt))
stop_ts  = int(time.mktime(stop_tt))
interval = 300

config = """
[DataBindings]
    
    [[wx_binding]]
        # The database must match one of the sections in [Databases].
        # This is likely to be the only option you would want to change.
        database = archive_sqlite
        # The name of the table within the database
        table_name = archive
        # The manager handles aggregation of data for historical summaries
        manager = weewx.wxmanager.WXDaySummaryManager
        # The schema defines the structure of the database.
        # It is *only* used when the database is created.
        schema = schemas.wview.schema

##############################################################################

#   This section defines various databases.

[Databases]
    
    # A SQLite database is simply a single file
    [[archive_sqlite]]
        database_name = year.sdb
        database_type = SQLite
    
    # MySQL
    [[archive_mysql]]
        database_name = year
        database_type = MySQL

##############################################################################

#   This section defines defaults for the different types of databases.

[DatabaseTypes]

    # Defaults for SQLite databases
    [[SQLite]]
        driver = weedb.sqlite
        # Directory in which the database files are located
        SQLITE_ROOT = /var/tmp/benchmarks
    
    # Defaults for MySQL databases
    [[MySQL]]
        driver = weedb.mysql
        # The host where the database is located
        host = localhost
        # The user name for logging in to the host
        user = weewx
        # The password for the user name
        password = weewx
"""

config_dict = configobj.ConfigObj(StringIO.StringIO(config))

def main():
    
    # First generate the fake data, using sqlite
    print "***** SQLITE *****"
    gen_fake_data.configDatabase(config_dict, 'wx_binding', 
                                 start_ts=start_ts, stop_ts=stop_ts, interval=interval)

    time_query(config_dict)
    vec = time_manager(config_dict)
    with open("manager_sqlite.out", "w") as fd:
        for x in vec:
            fd.write("%s %.2f\n" % (weeutil.weeutil.timestamp_to_string(x[0]), x[1]))
    vec = time_daily(config_dict)
    with open("daily_sqlite.out", "w") as fd:
        for x in vec:
            fd.write("%s %.2f\n" % (weeutil.weeutil.timestamp_to_string(x[0]), x[1]))
    
    # Now do it all again, but with MySQL
    print "***** MySQL *****"
    config_dict['DataBindings']['wx_binding']['database'] = 'archive_mysql'
    gen_fake_data.configDatabase(config_dict, 'wx_binding', 
                                 start_ts=start_ts, stop_ts=stop_ts, interval=interval)

    time_query(config_dict)
    vec = time_manager(config_dict)
    with open("manager_mysql.out", "w") as fd:
        for x in vec:
            fd.write("%s %.2f\n" % (weeutil.weeutil.timestamp_to_string(x[0]), x[1]))
    vec = time_daily(config_dict)
    with open("daily_mysql.out", "w") as fd:
        for x in vec:
            fd.write("%s %.2f\n" % (weeutil.weeutil.timestamp_to_string(x[0]), x[1]))

def time_query(config_dict):
    """A low-level approach. Use a SQL query"""
    
    manager_dict = weewx.manager.get_manager_dict_from_config(config_dict, 'wx_binding')
    # Just use a regular ol' manager to avoid using the daily summary optimization:
    manager_dict['manager'] = 'weewx.manager.Manager'

    manager = weewx.manager.open_manager(manager_dict,initialize=False)
    
    print "start time=", timestamp_to_string(start_ts)
    print "stop time=",  timestamp_to_string(stop_ts)
    print "Approximately %.1f months of data" % ((stop_ts - start_ts)/(30*24*3600))
    
    t0 = time.time()
    vec = []
    
    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        query = 'SELECT dateTime, outTemp FROM archive WHERE dateTime > %d AND dateTime <= %d '\
                    'AND outTemp = (SELECT MAX(outTemp) FROM archive WHERE dateTime > %d AND dateTime <= %d)' % (span + span)
        tup = manager.getSql(query)
        vec.append(tup)
    t1 = time.time()
    print "Elapsed query time=", t1-t0
        
    print vec
    
def time_manager(config_dict):
    """Use the class weewx.manager.Manager. This will actually require *two* queries. """
    manager_dict = weewx.manager.get_manager_dict_from_config(config_dict, 'wx_binding')
    # Just use a regular ol' manager to avoid using the daily summary optimization:
    manager_dict['manager'] = 'weewx.manager.Manager'

    manager = weewx.manager.open_manager(manager_dict,initialize=False)
    vec = run_manager(manager)
    return vec

def time_daily(config_dict):
    """Use the class weewx.wxmanager.WXDaySummaryManager, which knows how to use the daily
    summaries."""
    manager_dict = weewx.manager.get_manager_dict_from_config(config_dict, 'wx_binding')

    manager = weewx.manager.open_manager(manager_dict,initialize=False)

    vec = run_manager(manager)
    return vec

def run_manager(manager):    
    print "start time=", timestamp_to_string(start_ts)
    print "stop time=",  timestamp_to_string(stop_ts)
    print "Approximately %.1f months of data" % ((stop_ts - start_ts)/(30*24*3600))
    
    t0 = time.time()
    vec = []
    
    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        maxTemp = manager.getAggregate(span, 'outTemp', 'max')[0]
        ts = manager.getAggregate(span, 'outTemp', 'maxtime')[0]
        vec.append((ts, maxTemp))
        
    t1 = time.time()
    print "Elapsed query time=", t1-t0
        
    return vec

main()
