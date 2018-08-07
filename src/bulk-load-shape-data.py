import os
import glob
from datetime import datetime
import logging
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from decimal import Decimal
import shapefile
import json

logging.basicConfig(format='%(asctime)s %(message)s')


es = Elasticsearch("http://addresses_admin:addresses@LDVDEVLOGGER01:9200")

def getIndexName():
    return "openshape-02"

# Main Application Entry Point
def main():
    # for filename in glob.glob(os.path.join(getPathForCSVFiles(), '*.dbf', )):
    importShapeFiles("\\\\catlin.com\\data\\Global\\scratch\\ElasticSearch\\Switzerland Data\\gis_osm_buildings_ch\\gis_osm_buildings_ch")    


def importShapeFiles(filepath):
    actions = []
    indexable_object = {}
    
    sf = shapefile.Reader(filepath)

    # name of fields
    fields = sf.fields[1:] 
    field_names = [field[0] for field in fields] 

    shapeRecs = sf.iterShapeRecords()
    for shapeRec in shapeRecs:
        # Create indexable object
        indexable_object = dict(zip(field_names, shapeRec.record))
        indexable_object["shape"] = shapeRec.shape.__geo_interface__ 

        # remove the name and type as completion fields cannot be blank
        if indexable_object["name"] == "" : del indexable_object["name"]
        if indexable_object["type"] == "" : del indexable_object["type"]

        # format an Elastic Action
        action = {
            '_index': getIndexName(),
            '_op_type': 'index',
            '_type': 'doc',
            '_id': indexable_object["osm_id"],
            '_source': indexable_object
        }

        actions.append(action)
        # Flush bulk indexing action if necessary
        if len(actions) >= 5000:
            bulkLoadintoElastic(actions)

    # Flush bulk indexing action if necessary
    bulkLoadintoElastic(actions)


def bulkLoadintoElastic(actions):
    if len(actions) > 0:
        logging.warn("Indexing batch of " + str(len(actions)))
        helpers.bulk(es, actions)
        del actions[0:len(actions)]


if __name__ == "__main__":
    main()