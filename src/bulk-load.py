import os
import glob
import csv
from datetime import datetime
import logging
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from decimal import Decimal
import json

logging.basicConfig(format='%(asctime)s %(message)s')

fieldnames = ("id","number","street","unit","city","district","region","region_name","postcode","country_code","longitude","latitude")

es = Elasticsearch("http://addresses_admin:addresses@LDVDEVLOGGER01:9200")

def getIndexName():
    return "opencountrywide-01"

def getPathForCSVFiles():
    return "\\\\catlin.com\\data\\Global\\Scratch\\ElasticSearch\\Switzerland Data\\switzerland-countrywide_mod\\"

# Main Application Entry Point
def main():
    for filename in glob.glob(os.path.join(getPathForCSVFiles(), '*.csv', )):
        importCSVfile(filename)    

def importCSVfile(filename):
    actions = []
    indexable_object = {}
    
    with open(filename, 'r') as csvfile:  
        reader = csv.DictReader(csvfile, fieldnames)
        for row in reader:
            # Skip first row
            if row['id'] != "id":
                # Create indexable object
                indexable_object = row
                try:
                    indexable_object['location'] = {
                        'lat' : float(indexable_object['latitude']),
                        'lon' : float(indexable_object['longitude'])
                    }
                except Exception:
                    pass  # or you could use 'continue'
                
                indexable_object = json.dumps( indexable_object )
                
                # format an Elastic Action
                action = {
                    "_index": getIndexName(),
                    '_op_type': 'index',
                    "_type": 'doc',
                    "_source": indexable_object
                }
                actions.append(action)
                # Flush bulk indexing action if necessary
                if len(actions) >= 4500:
                    bulkLoadintoElastic(actions, filename)

        # Flush bulk indexing action if necessary
        bulkLoadintoElastic(actions, filename)


def bulkLoadintoElastic(actions, filename):
    if len(actions) > 0:
        logging.warn("Indexing batch of " + str(len(actions)) + " ..." + filename)
        helpers.bulk(es, actions)
        
        del actions[0:len(actions)]

def fixEmptyFields(data, fields):
    for field in fields:
        if not field in data or len(data[field]) == 0:
            data[field] = "blank"
    return data


if __name__ == "__main__":
    main()