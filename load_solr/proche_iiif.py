import pandas as pnd
import httplib2
import json
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET
import re
from requests import post
from requests import get
#from requests.auth import HTTPBasicAuth, HTTPDigestAuth, HTTPProxyAuth
import base64

SRC_FILE="D:\\DEV\\PROCHE\\python\\test_iiif.txt"

h = httplib2.Http(".cache")

h.add_credentials('', '')
credentials = ('%s:%s' % ('', ''))
encoded_credentials = base64.b64encode(credentials.encode('utf-8'))


SOLR_URL='https://proche.africamuseum.be/solradmin/solr/proche-prod/'

df_main=pnd.read_csv(SRC_FILE, sep="\t", header=0)


def basic_auth(username, password):
    token = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
    return f'Basic {token}'
    
    
def find_id(p_h, p_solr_url, key):
    query_url=p_solr_url+"select?indent=true&q.op=AND&q=object_number%3A"+key+"&useParams=&wt=json"
    resp, content = p_h.request(query_url, "GET",  headers={'content-type':'application/xml', 'charset':'utf-8'} )
    #resp= json.loads(resp.decode('utf-8'))
    content= json.loads(content.decode('utf-8'))
    data =None
    
    if resp["status"]=='200':       
        if "response" in content:
            if "docs" in content["response"]:
                if len(content["response"]['docs'])>0:
                    data=content["response"]['docs'][0]
    return data

def insert_solr(p_h, p_solr_url,  p_fields ):
    commit_url=p_solr_url+"update?commit=true"
    print(commit_url)
    #resp, content = p_h.request(commit_url, "GET",  headers={'content-type':'application/xml', 'charset':'utf-8'} , body=json.dumps(p_fields))
    
    print(p_fields)
    p_fields=[p_fields]
    print(p_fields)
    #tmp=get(commit_url, auth=cred)
    #print(tmp)
    resp = post(commit_url,  headers={'content-type':'application/json', 'charset':'utf-8','Authorization':basic_auth('proche_admin', 'pr0#µeb202E')}, data=json.dumps(p_fields).encode("ascii") )
    print(resp)
    print(resp.text)    

    
for i, row in df_main.iterrows():
    print(i)
    print(row)
    data=find_id(h, SOLR_URL, row["object_id"])
    #print(data)
    if data is not None:
        data2 = data.copy()
        for key in data:
            if key.endswith('_str'):
                if key !="year_of_acquisition_str":
                    del data2[key]
        data2["iiif_endpoint"]=row["iiif_endpoint"]
        insert_solr(h, SOLR_URL,  data2 )
    else:
        print("ERROR id of object not found")