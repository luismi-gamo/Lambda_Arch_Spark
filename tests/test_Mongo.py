import Definitions
from pymongo import MongoClient



client = MongoClient(Definitions.MONGO_LOCATION)
db = client.lambdaDB
# #inserts elements
# saved_word = db.PowerCount_Ruina.insert_many([
#     {
#         "meridian": "10",
#         "index": "1.6",
#         "lab": "Szajna",
#         "count": 10
#     }, {
#         "meridian": "12",
#         "index": "1.6",
#         "lab": "Szajna",
#         "count": 5
#         }
#     ])
#Updates or inserts
updatereg = db['PowerCount_Ruina'].update_one(
    {
        "meridian": "10",
        "index": "1.6",
        "lab": "Szajna",
    }, {
        '$inc': { 'count': 5}
    },
    upsert = True
)
