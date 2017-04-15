import sqlite3
import random

def dropPricesTable(db, table):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute('DROP TABLE '+ table)
    conn.commit()
    conn.close()

def createPricesTable(db, table):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute('CREATE TABLE ' + table + ' (lab_name CHAR(200), product_name CHAR(50), price FLOAT)')
    # Duarte
    c.execute("INSERT INTO " + table + " VALUES ('Duarte','Alpha', 6.0)")
    c.execute("INSERT INTO " + table + " VALUES ('Duarte','FreeStyle',9.95)")
    #Greiche&Scaff
    c.execute("INSERT INTO " + table + " VALUES ('Greiche&Scaff','Alpha', 2.5)")
    c.execute("INSERT INTO " + table + " VALUES ('Greiche&Scaff','FreeStyle',4.0)")
    # LOA
    c.execute("INSERT INTO " + table + " VALUES ('LOA','Alpha', 2.25)")
    c.execute("INSERT INTO " + table + " VALUES ('LOA','FreeStyle',3.65)")
    # Montroyal
    c.execute("INSERT INTO " + table + " VALUES ('Montroyal','Alpha', 3.15)")
    c.execute("INSERT INTO " + table + " VALUES ('Montroyal','FreeStyle',5.15)")
    # SpecSavers
    c.execute("INSERT INTO " + table + " VALUES ('SpecSavers','Alpha', 2.15)")
    c.execute("INSERT INTO " + table + " VALUES ('SpecSavers','FreeStyle',2.95)")
    #Szajna
    c.execute("INSERT INTO " + table + " VALUES ('Szajna','Alpha', 2.5)")
    c.execute("INSERT INTO " + table + " VALUES ('Szajna','FreeStyle',4.75)")
    #Visionlab
    c.execute("INSERT INTO " + table + " VALUES ('Visionlab','Alpha', 3.2)")
    c.execute("INSERT INTO " + table + " VALUES ('Visionlab','FreeStyle',6.75)")
    #Visionworks
    c.execute("INSERT INTO " + table + " VALUES ('Visionworks','Alpha', 6.2)")
    c.execute("INSERT INTO " + table + " VALUES ('Visionworks','FreeStyle',10.75)")
    #Vitolen
    c.execute("INSERT INTO " + table + " VALUES ('Vitolen','Alpha', 7.15)")
    c.execute("INSERT INTO " + table + " VALUES ('Vitolen','FreeStyle',11.0)")
    # VSP
    c.execute("INSERT INTO " + table + " VALUES ('VSP','Alpha', 7.5)")
    c.execute("INSERT INTO " + table + " VALUES ('VSP','FreeStyle', 10.75)")
    conn.commit()
    conn.close()

def viewPricesTable(db, table):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    for r in c.execute("SELECT * FROM "+ table):
        print r
    conn.commit()
    conn.close()

#Loads the DB table into a list of dictionaries
def loadPricesTable(db, table):
    pricesList = list()
    conn = sqlite3.connect(db)
    c = conn.cursor()
    for r in c.execute("SELECT * FROM "+ TABLE):
        labDict = dict()
        labDict['laboratory'] = r[0]
        labDict['product'] = r[1]
        labDict['price_euro'] = r[2]
        pricesList.append(labDict)
    conn.commit()
    conn.close()
    return pricesList

#Returns the price as float
def findPriceForLabAndProduct(lab, prod, prodlist):
    for p in prodlist:
        if (p['laboratory'] == lab and p['product'] == prod):
            return p['price_euro']

    return 0.0

#defines a probability of being a redo function of the laboratory
def isRedo(lab):
    if lab == 'Visionlab':
        return int(random.randint(0,200) == 0)
    elif lab == 'VSP':
        return int(random.randint(0, 40) == 0)
    elif lab == 'Visionworks':
        return int(random.randint(0, 4) == 0)
    else:
        return int(random.randint(0, 15) == 0)


DB_LOCATION = '../db/Prices.db'
TABLE = 'Click_Fees_Lab'
pricesList = list()
if __name__ == "__main__":
    #dropPricesTable(DB_LOCATION, TABLE)
    #createPricesTable(DB_LOCATION, TABLE)
    #viewPricesTable(DB_LOCATION, TABLE)
    pricesList = loadPricesTable(DB_LOCATION, TABLE)
    price = findPriceForLabAndProduct('LOA', "Alpha", pricesList)
    print price
