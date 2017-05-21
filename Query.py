import sqlite3
import threading
import pandas as pd
from pandas import Series, DataFrame
import matplotlib.pyplot as plt
import time
from pymongo import MongoClient

class QueryClass(threading.Thread):
    # Singleton for DB connections
    bd = None
    mongodb = None

    def __init__(self, db, mongodb, sleeptime):
        threading.Thread.__init__(self)
        self.name = "query"
        # Sets the DB parameters
        self.db = db
        if QueryClass.bd is None:
            QueryClass.bd = db
        self.mongodb = mongodb
        if QueryClass.mongodb is None:
            QueryClass.mongodb = mongodb
        # al iniciar el flujo activo es el 1
        self.active = 1
        #Time to be inactive before querying the database for changes
        self.sleeptime = sleeptime

    def run(self):

        while True:
            bview = QueryClass.readBView(QueryClass.dbConnection())
            rtview = QueryClass.readRTView(QueryClass.dbConnection(), self.active)
            if bview.size != 0 and rtview.size != 0:
                total_df = bview.merge(rtview, on=['meridian', 'lab', 'index'], how='left').fillna(0)
                total_df['count'] = total_df['count_x'] + total_df['count_y']
                #print total_df
                total_df.to_sql("PowerCount_Total", QueryClass.dbConnection(), if_exists = 'replace', index = False)
                print "\nwrote PowerCount_Total = PowerCount_bv + PowerCount_rt" + str(self.active)
            else:
                print "\nQuery: No data available"
            time.sleep(self.sleeptime)


    def changeTable(self):
        if self.active == 1:
            self.active = 2
        else:
            self.active = 1

    @staticmethod
    def dbConnection():
        return sqlite3.connect(QueryClass.bd)

    @staticmethod
    def mongodbConnection():
        return MongoClient(QueryClass.mongodb)

    @staticmethod
    def readBView(conn):
        mongoconn = QueryClass.mongodbConnection()
        db = mongoconn.lambdaDB
        # Make a query to the specific DB and Collection
        cursor = db['PowerCount_bv'].find({})
        # Expand the cursor and construct the DataFrame
        df = pd.DataFrame(list(cursor))
        # Delete the _id
        if df.size != 0:
            del df['_id']
        #print df
        return df
        #return pd.read_sql_query("SELECT * from PowerCount_bv", conn)

    @staticmethod
    def readRTView(conn, activeTable):
        collection = "PowerCount_rt" + str(activeTable)
        mongoconn = QueryClass.mongodbConnection()
        db = mongoconn.lambdaDB
        # Make a query to the specific DB and Collection
        cursor = db[collection].find({})
        # Expand the cursor and construct the DataFrame
        df = pd.DataFrame(list(cursor))
        # Delete the _id field
        if df.size != 0:
            del df['_id']
        #print df
        return df
        # query = "SELECT * from PowerCount_rt" + str(activeTable)
        # return pd.read_sql_query(query, conn)

# consulta = QueryClass(DB_LOCATION)
# consulta.start()