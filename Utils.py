import datetime



# Converts a dictionary with a lens design to a string 'name_type' formatted. Used to count products on windows
#"design": [{"type": "D", "name": "FreeStyle"}]
def productAsString(dict):
    return dict['name'] + "_" + dict['type']

# To count products in a window they are identified by a  string 'name_type' formatted.
# To save to DB we need to create a new tuple formatted as: (name, type, count)
def productStringAsTuple(product):
    resStr = product[0]
    count = product[1]
    result = resStr.split('_')
    return (result[0], result[1], count)