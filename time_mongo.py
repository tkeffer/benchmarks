"""Time how long it takes to build a vector with the daily max temperature
for each day in a year, along with its time, using MongoDB """
import sys
import os
import datetime
import time

import pymongo
from weeutil.weeutil import timestamp_to_string
import weeutil.weeutil

from gen_fake_data import genFakeRecords

os.environ['TZ'] = 'America/Los_Angeles'

# Twelve months of data:
start_tt = (2010, 1, 1, 0, 0, 0, 0, 0, -1)
stop_tt = (2011, 1, 1, 0, 0, 0, 0, 0, -1)
start_ts = int(time.mktime(start_tt))
stop_ts = int(time.mktime(stop_tt))
interval = 300

def main():
    # Get a client...
    client = pymongo.MongoClient()
    # ... and a database...
    db = client.example
    # ... then finally a collection, called 'archive'
    collection = db.archive

    # Generate the data    
    gen_data(collection)

    # Now time a query:
    time_query(collection)
        
def gen_data(collection):
    """Function for generating fake data and putting it in the MongoDB database."""
    
    # Drop any existing collection, then rebuild it
    collection.drop()
    
    t0 = time.time()
    N = 0
    for record in genFakeRecords(start_ts=start_ts, stop_ts=stop_ts, interval=interval):
        if not N % 100:
            print >> sys.stdout, "packets processed: %d; Last date: %s\r" % \
                (N, weeutil.weeutil.timestamp_to_string(record["dateTime"])),
            sys.stdout.flush()
        record['dateTime'] = datetime.datetime.utcfromtimestamp(record['dateTime'])
        collection.insert_one(record)
        N += 1
    # Build an index on the timestamp
    collection.create_index("dateTime", unique=True)
    t1 = time.time()
    print "Finished creating collection. Elapsed time ", (t1 - t0)

def time_query(collection):
    """Time how long it takes to create a vector with daily max temperatures."""
    
    print "start time=", timestamp_to_string(start_ts)
    print "stop time=", timestamp_to_string(stop_ts)
    print "(Approximately %.1f months of data)" % ((stop_ts - start_ts) / (30 * 24 * 3600))
    
#     # This would give the max temperature in each UTC day (unfortunately
#     # we want the max temperature in each local day)
#     rs = collection.aggregate([{ "$project": {"day": { "$dayOfYear": "$dateTime" },
#                                               "dateTime": 1,
#                                               "outTemp": 1
#                                               }},
#                                { "$sort": { "day": 1, "outTemp":-1 } },
#                                { "$group": {
#                                             "_id" : "$day",
#                                             "max_temperature": { "$first": "$outTemp" },
#                                             "timestamp": { "$first": "$dateTime" }
#                                             }},
#                                { "$sort": { "_id":1 } }
#                                ])


    epoch = datetime.datetime.utcfromtimestamp(0)

    vec = []
    t0 = time.time()
    for span in weeutil.weeutil.genDaySpans(start_ts, stop_ts):
        rs = collection.aggregate([{"$match"   : {"dateTime" : {"$gt"  : datetime.datetime.utcfromtimestamp(span[0]), 
                                                                "$lte" : datetime.datetime.utcfromtimestamp(span[1])},
                                                  "outTemp"  : {"$ne" : None}
                                                  }}, 
                                   {"$project" : {"dateTime" : 1, "outTemp" : 1}},
                                   {"$sort" : {"outTemp" : -1 } },
                                   {"$limit" : 1}
                                   ])
        for x in rs:
            # Convert from a (timezone naive) datetime object to unix epoch time: 
            delta = x['dateTime'] - epoch
            vec.append((delta.total_seconds(), x['outTemp']))

    t1 = time.time()
    print "Elapsed query time=", (t1 - t0)
    print vec
 
    with open("mongo.out", "w") as fd:
        for x in vec:
            fd.write("%s %.2f\n" % (weeutil.weeutil.timestamp_to_string(x[0]), x[1]))
main()
