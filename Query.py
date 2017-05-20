import sqlite3
import threading
import pandas as pd
from pandas import Series, DataFrame
import matplotlib.pyplot as plt
import time

class QueryClass(threading.Thread):
    # Singleton for DB connections
    bd = None

    def __init__(self, db, sleeptime):
        threading.Thread.__init__(self)
        self.name = "query"
        # Sets the DB parameters
        self.db = db
        if QueryClass.bd is None:
            QueryClass.bd = db
        # al iniciar el flujo activo es el 1
        self.active = 1
        #Time to be inactive before querying the database for changes
        self.sleeptime = sleeptime

    def run(self):

        while True:
            bview = QueryClass.readBView(QueryClass.dbConnection())
            rtview = QueryClass.readRTView(QueryClass.dbConnection(), self.active)

            total_df = bview.merge(rtview, on=['meridian', 'lab', 'n_index'], how='left').fillna(0)
            total_df['count'] = total_df['count_x'] + total_df['count_y']
            total_df.to_sql("PowerCount_Total", QueryClass.dbConnection(), if_exists = 'replace')
            #print "wrote PowerCount_Total"
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
    def readBView(conn):
        return pd.read_sql_query("SELECT * from PowerCount_bv", conn)

    @staticmethod
    def readRTView(conn, activeTable):
        query = "SELECT * from PowerCount_rt" + str(activeTable)
        return pd.read_sql_query(query, conn)

# consulta = QueryClass(DB_LOCATION)
# consulta.start()