import sqlite3
import threading
import pandas as pd
from pandas import Series, DataFrame
import matplotlib.pyplot as plt
import time

DB_LOCATION = 'db/LensesDB.db'

class QueryClass(threading.Thread):
    # Singleton for DB connections
    bd = None

    def __init__(self, db):
        threading.Thread.__init__(self)
        self.name = "query"
        # Sets the DB parameters
        self.db = db
        if QueryClass.bd is None:
            QueryClass.bd = db

    def run(self):

        while True:
            bview = QueryClass.readBView(QueryClass.dbConnection())
            rtview = QueryClass.readRTView(QueryClass.dbConnection())

            total_df = bview.merge(rtview,on='name',how='outer').fillna(0)
            total_df['count'] = total_df['count_x'] + total_df['count_y']
            #print total_df

            ax = total_df.plot(x='name', y='count',kind='bar',label='Contador',
                        title='Merge',color='g')
            ax.set_xlabel('Products')
            ax.set_ylabel('Total Count')
            plt.show()
            time.sleep(10)
            #plt.close()

    @staticmethod
    def dbConnection():
        return sqlite3.connect(QueryClass.bd)

    @staticmethod
    def readBView(conn):
        return pd.read_sql_query("SELECT * from ProductCount_bv", conn)

    @staticmethod
    def readRTView(conn):
        return pd.read_sql_query("SELECT * from ProductCount_rt1", conn)

consulta = QueryClass(DB_LOCATION)
consulta.start()