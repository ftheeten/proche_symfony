import pyodbc
import httplib2
import datetime
import traceback
import sys
import pandas as pnd
import json
from collections import OrderedDict
import sqlalchemy as sa
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET
import re
import urllib

 
print("init")
 

 
h = httplib2.Http(".cache")
h.add_credentials('', '')



global_terms={}
solr_url='https://proche.africamuseum.be/solradmin/solr/proche-constituents/'
main_filter="  "
 
 
 
#--------------------
def insert_solr(p_h, p_solr_url,  p_fields, list_multi_fields=None ):
    list_fields=[]
    insert_url=p_solr_url+"update"
    commit_url=p_solr_url+"update?commit=true"
    for k, v in p_fields.items():
        if not v is None:
            list_fields.append("<field name='"+k+"'>"+escape(str(v))+"</field>")
            
    
    if not list_multi_fields is  None:
        for k, tmp in list_multi_fields.items():
            for v in tmp:
                list_fields.append("<field name='"+k+"'>"+escape(str(v))+"</field>")
    
    xml="<add><doc>"+"".join(list_fields)+"</doc></add>"
    #print(xml)
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
        
def test_connection(cn, constr):
    rebuild=False
    if cn is None:
        rebuild=True
    elif not cn:
        rebuild=True
    if not rebuild:
        cn= sa.create_engine(constr)
    return cn

 
def get_constituents(conn):
    sql="with a as   (     SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1     WHERE PackageID =130507 or  PackageID =130506 or PackageID =130508  or  PackageID =130509   ),      c_phys     AS     (SELECT * FROM [TMS].[dbo].[Constituents] WHERE  ConstituentTypeID = 1 OR ConstituentTypeID=3),    c_mor     AS     (SELECT * FROM  [TMS].[dbo].[Constituents] WHERE  ConstituentTypeID = 2 OR ConstituentTypeID=4),   b    AS    (    SELECT  c_phys.DisplayName, c_phys.BeginDate, c_phys.EndDate,  ConstituentTypeID, c_phys.ConstituentID,  c_phys.Biography , BeginDateISO, EndDateISO, Nationality  FROM [TMS].[dbo].[vConXrefsAll]    INNER JOIN c_phys   ON vConXrefsAll.ConstituentID = c_phys.ConstituentID    INNER JOIN a ON    a.[ID]=vConXrefsAll.ID    WHERE [vConXrefsAll].TableID = 108    AND vConXrefsAll.Active = 1    AND (RoleTypeID IN (2, 5) OR (RoleTypeID = 1 AND RoleID = 1))    AND ( endDate !=0 OR (EndDate=0 and BeginDate< 1910 and ConstituentTypeID=1) )      )   ,   c   AS   (    SELECT  'NOT_DISPLAYED' DisplayName, NULL BeginDate, NULL EndDate,  ConstituentTypeID, c_phys.ConstituentID   ,  Biography , BeginDateISO, EndDateISO, Nationality  FROM [TMS].[dbo].[vConXrefsAll]    INNER JOIN c_phys   ON vConXrefsAll.ConstituentID = c_phys.ConstituentID    INNER JOIN a ON    a.[ID]=vConXrefsAll.ID    WHERE [vConXrefsAll].TableID = 108    AND vConXrefsAll.Active = 1    AND    (RoleTypeID IN (2, 5) OR (RoleTypeID = 1 AND RoleID = 1))    AND    NOT (    endDate !=0    OR (EndDate=0 and BeginDate< 1910 and ConstituentTypeID=1)    )),    d   AS   (    SELECT c_mor.DisplayName, c_mor.BeginDate, c_mor.EndDate,  ConstituentTypeID, c_mor.ConstituentID   ,  Biography , BeginDateISO, EndDateISO, Nationality  FROM [TMS].[dbo].[vConXrefsAll]    INNER JOIN c_mor   ON vConXrefsAll.ConstituentID = c_mor.ConstituentID    INNER JOIN a ON    a.[ID]=vConXrefsAll.ID    WHERE [vConXrefsAll].TableID = 108    AND vConXrefsAll.Active = 1    AND (RoleTypeID IN (2, 5) OR (RoleTypeID = 1 AND RoleID = 1))    )   ,   e   AS   (   SELECT *  FROM b       UNION    SELECT * FROM d)   SELECT distinct * FROM e   "
    data=pnd.read_sql(sql=sql, con=conn)
    return data
       
#----------------------------main 
 
 

params = urllib.parse.quote_plus(r'Driver={ODBC Driver 18 for SQL Server};Server=,1433;Database=;Uid=;Pwd=$;TrustServerCertificate=yes;')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
cn = sa.create_engine(conn_str)

 
 
print("run")
print_time()

pnd_constituents=get_constituents(cn)
print("Got constituents")
print_time()

print(pnd_constituents)

for index, row in pnd_constituents.iterrows():
    try:
        print(row)
        doc={
            "id":row["ConstituentID"],
            "display_name_sort": row["DisplayName"] or "",
            "display_name": row["DisplayName"] or "",
            "biography": row["Biography"] or "",
            "nationality":  row["Nationality"] or "",
        }
        if row["BeginDateISO"] is not None:
            doc["birth_date"]=row["BeginDateISO"]
            doc["birth_year"]=row["BeginDateISO"][:4]
        if row["EndDateISO"] is not None:
            doc["death_date"]=row["EndDateISO"]
            doc["death_year"]=row["EndDateISO"][:4]
        insert_solr(h, solr_url, doc)
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
