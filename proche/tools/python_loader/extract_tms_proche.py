import pyodbc
import httplib2
import datetime
import traceback
import sys
import pandas as pnd
from collections import OrderedDict
import sqlalchemy as sa
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET
 
print("init")
 
 
h = httplib2.Http(".cache")
h.add_credentials('', '')


global_terms={}
solr_url='https://proche.africamuseum.be/solradmin/solr/proche-prod/'
main_filter=" 1=1 "
 
 
def insert_solr(p_h, p_solr_url,  p_fields ):
    list_fields=[]
    insert_url=p_solr_url+"update"
    commit_url=p_solr_url+"update?commit=true"
    for k, v in p_fields.items():
        if not v is None:
            list_fields.append("<field name='"+k+"'>"+escape(str(v))+"</field>")
    xml="<add><doc>"+"".join(list_fields)+"</doc></add>"
    resp, content = p_h.request(insert_url, "GET", body=xml.encode('utf-8'), headers={'content-type':'application/xml', 'charset':'utf-8'} )
    check_xml = ET.fromstring(content)
    stat=check_xml.findall(".//int[@name='status']")
    if(len(stat)>0):
        if str(stat[0].text).strip()!="0":
            print("Error - return code ="+ str(stat[0].text).strip() )
            print(p_fields)
            print(xml)
            print(content)
    else:
        print("Error no return code")
        print(p_fields)
        print(xml)
    resp2, content2= p_h.request(commit_url)


def test_connection(cn, constr):
    rebuild=False
    if cn is None:
        rebuild=True
    elif not cn:
        rebuild=True
    if not rebuild:
        cn= sa.create_engine(constr)
    return cn
   
    
 
def get_tombstone(conn):
    sql="With c as \
  ( SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1 WHERE "+ main_filter+ ")\
SELECT t.[ObjectID], t.[ObjectNumber] , t.[SortNumber], t.Medium, t.Dimensions , t.Title FROM \
dbo.[vgsrpObjTombstoneD_RO] t  INNER JOIN c ON c.[ID]=t.[ObjectID]"
    data=pnd.read_sql(sql=sql, con=conn)
    return data
   
def get_sites(conn):
    sql="With c as \
  ( SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1 WHERE "+ main_filter+ ")\
SELECT t.[ID] as [ObjectID], t.[SitesOfCollectionFlat] , t.[SitesOfProductionFlat]  FROM \
dbo.[vRmcaLvObjectsGeography ] t  INNER JOIN c ON c.[ID]=t.[ID]"
    data=pnd.read_sql(sql=sql, con=conn)
    return data
   
def get_acquisition_metadata(conn):
    sql="With c as \
  ( SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1 WHERE "+ main_filter+ ")\
SELECT t.[ID] as [ObjectID], t.[AccessionISODate] , t.[AccessionMethod]  FROM \
dbo.[vRmcaLvObjectsAcquisitionConstituents] t  INNER JOIN c ON c.[ID]=t.[ID]"
    data=pnd.read_sql(sql=sql, con=conn)
    return data
 
def print_time():
    now = datetime.datetime.now()
    print ("Current date and time : ")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))
 
def prepare_json_object(data):
    if data is None:
        return None
    elif len(str(data).strip())==0:
        return None
    else:
        return str(data).strip()
       
    
def create_doc(id, objnumber, sort_number,  title, material_and_technique, dimensions, site_of_collection, site_of_production, date_of_acquisition, method_of_acquisition, creation_date):
    year=None
    if date_of_acquisition is not None:
        if len(str(date_of_acquisition).strip())>4:
            year_tmp=date_of_acquisition[0:4]
            if str(year_tmp).isnumeric():
                if len(year_tmp.strip())==4:
                    year=int(year_tmp)
    doc={
        "id":id,
        "object_number":prepare_json_object(objnumber),
        "sort_number":prepare_json_object(sort_number),
        "title":prepare_json_object(title),
        "material_and_technique":prepare_json_object(material_and_technique),
        "dimensions": prepare_json_object(dimensions),
        "site_of_collection" : prepare_json_object(site_of_collection),
        "site_of_production" : prepare_json_object(site_of_production),
        "date_of_acquisition":  prepare_json_object(date_of_acquisition),
        "method_of_acquisition": prepare_json_object(method_of_acquisition),
        
        "creation_date":creation_date
    }
    if year is not None:
         doc["year_of_acquisition"]= int(year)
         doc["year_of_acquisition_str"]= str(year)
    return doc
   

 
cn = sa.create_engine('mssql+pyodbc://db/TMS?driver=ODBC Driver 17 for SQL Server')

 
 
print("run")
print_time()
main_data=get_tombstone(cn)
print("Got main data")
print_time()
print(len(main_data))
pnd_sites=get_sites(cn)
print("Got sites")
print_time()
print(len(pnd_sites))
pnd_acq_metadata=get_acquisition_metadata(cn)
print("Got sites")
print_time()
print(len(pnd_acq_metadata))
cn.dispose()
 
p_all=main_data.merge(pnd_sites, on='ObjectID')
p_all=p_all.merge(pnd_acq_metadata, on='ObjectID')
 
for index, row in p_all.iterrows():
    try:
        #print(index)
        #print(row)
        doc=create_doc( row["ObjectID"], row["ObjectNumber"], row["SortNumber"], row["Title"], row["Medium"], row["Dimensions"] , row["SitesOfCollectionFlat"] , row["SitesOfProductionFlat"] , row["AccessionISODate"] , row["AccessionMethod"] , datetime.datetime.now().isoformat())
        #print(doc)
        #print(i)
        insert_solr(h, solr_url, doc)
        if index%100==0:
            print(index)
        #if index>100:
        #    break 
    except BaseException as ex:
        # Get current system exception
        print("Exception line %s", str(index))
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
        print("Exception type : %s " % ex_type.__name__)
        print("Exception message : %s" %ex_value)
        print("Stack trace : %s" %stack_trace)
    except KeyboardInterrupt as ex:
        sys.exit()
   