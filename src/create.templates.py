import sys
from elasticsearch import helpers
from elasticsearch import Elasticsearch

es = Elasticsearch("http://addresses_admin:addresses@LDVDEVLOGGER01:9200")

with open('..\\templates\\openaddress.json', 'r') as file:
    template=file.read().replace('\n', '')
    file.close() 

print("Creating openaddresses template...")
res = es.indices.put_template(name="openaddresses-*", body=template, order=10)
print(res)
print("Completed")

with open('..\\templates\\openshape.json', 'r') as file:
    template=file.read().replace('\n', '')
    file.close() 

print("Creating openshape template...")
res = es.indices.put_template(name="openshape-*", body=template, order=10)
print(res)
print("Completed")

with open('..\\templates\\opencountrywide.json', 'r') as file:
    template=file.read().replace('\n', '')
    file.close() 

print("Creating opencountrywide template...")
res = es.indices.put_template(name="opencountrywide-*", body=template, order=10)
print(res)
print("Completed")
