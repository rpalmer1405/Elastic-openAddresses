import os
import glob
import csv
from datetime import datetime
import logging
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from decimal import Decimal

logging.basicConfig(format='%(asctime)s %(message)s')

fieldnames = ("LON","LAT","NUMBER","STREET","UNIT","CITY","DISTRICT","REGION","POSTCODE","ID","HASH")

es = Elasticsearch("http://addresses_admin:addresses@LDVDEVLOGGER01:9200")

def getIndexName():
    return "openaddresses-03"

def getPathForCSVFiles():
    return "..\\openaddr-collected-us_northeast\\us\\**\\"

# Main Application Entry Point
def main():
    for filename in glob.glob(os.path.join(getPathForCSVFiles(), '*.csv', )):
        importCSVfile(filename)    

def importCSVfile(filename):
    actions = []
    indexable_object = {}
    
    # split the filename. part 3 = region, part 4 = filename
    fileparts = filename.split("\\")

    with open(filename, 'r') as csvfile:  
        reader = csv.DictReader(csvfile, fieldnames)
        for row in reader:
            # Skip first row
            if row['DISTRICT'] != "DISTRICT":
                # Create indexable object
                indexable_object = enrichAddressObjectWithRegionIfBlank(
                    enrichAddressObjectWithFileName(
                        createAddressObject(row),
                        fileparts[4]
                        ),
                    fileparts[3])    
                # format an Elastic Action
                action = {
                    "_index": getIndexName(),
                    '_op_type': 'index',
                    "_type": 'doc',
                    "_source": indexable_object
                }
                actions.append(action)
                # Flush bulk indexing action if necessary
                if len(actions) >= 5000:
                    bulkLoadintoElastic(actions, filename)

        # Flush bulk indexing action if necessary
        bulkLoadintoElastic(actions, filename)


def bulkLoadintoElastic(actions, filename):
    if len(actions) > 0:
        logging.warn("Indexing batch of " + str(len(actions)) + " ..." + filename)
        helpers.bulk(es, actions)
        del actions[0:len(actions)]

def enrichAddressObjectWithRegionIfBlank(address_object, region):
    if not (address_object['Region'] is None): address_object["Region"] = region
    return address_object

def enrichAddressObjectWithFileName(address_object, filename):
    address_object["Filename"] = filename
    return address_object

def createAddressObject(row):
    address_object = {}
    if not (row['NUMBER'] is None): address_object["Number"] = row['NUMBER']
    if not (row['STREET'] is None): address_object["Street"] = row['STREET']
    address_object["AddressLineOne"] = row['NUMBER'] + ", " + row['STREET']
    if not (row['UNIT'] is None): address_object["Unit"] = row['UNIT']
    if not (row['CITY'] is None): address_object["City"] = row['CITY']
    if not (row['DISTRICT'] is None): address_object["District"] = row['DISTRICT']
    if not (row['REGION'] is None): address_object["Region"] = row['REGION']
    if not (row['POSTCODE'] is None): address_object["PostCode"] = row['POSTCODE']
    if not (row['ID'] is None): address_object["Id"] = row['ID']
    if not (row['HASH'] is None): address_object["Hash"] = row['HASH']
    address_object["Location"] = {
            "lat": Decimal(row['LAT']),
            "lon": Decimal(row['LON'])
    }
    return address_object

if __name__ == "__main__":
    main()