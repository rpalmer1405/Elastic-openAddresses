import csv   
from datetime import datetime
from elasticsearch import helpers
from elasticsearch import Elasticsearch
es = Elasticsearch("http://addresses_admin:addresses@LDVDEVLOGGER01:9200")

sourceIndexName = "openshape-02"

query={
    "query": {
        "match_all": {}
    }
}

fields=['osm_id','fclass','area_sm']
#csv_file = open('..\\temp\\theframe-addresses-results.csv', 'a')
csv_file = open('.\\results.csv', 'w',  newline='', encoding='utf-8')
csv_writer = csv.writer(csv_file, fields)
csv_writer.writerow(fields)

def formatResult(hit):
    formatted_result = []
    formatted_result.append(hit['_source']['osm_id'])
    formatted_result.append(hit['_source']['fclass'])
    formatted_result.append(hit['_source']['area_sm'])
    return formatted_result

# Initialize the scroll
res = es.search(index=sourceIndexName, scroll="90s", size = 10, body=query)

scroll_id = res['_scroll_id']
scroll_size = res['hits']['total']
hits_left = scroll_size

# Start scrolling
while (scroll_size > 0):
    #print("Querried %d Hits: %d left" % res['hits']['total'], hits_left)
    for hit in res['hits']['hits']:
        result = formatResult(hit)
        #print(result)
        csv_writer.writerow(result)

    res = es.scroll(scroll_id = scroll_id, scroll = '2m')
    # Update the scroll ID
    sid = res['_scroll_id']
    # Get the number of results that we returned in the last scroll
    scroll_size = len(res['hits']['hits'])
    hits_left = hits_left - scroll_size
    # print ("scroll size: " + str(scroll_size))
    # Do something with the obtained page

es.clear_scroll(scroll_id)
csv_file.close()
#for hit in res['hits']['hits']:
#    print("%(MessageUUID)s" % hit["_source"])
