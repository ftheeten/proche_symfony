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
 
print("init")
 
 
h = httplib2.Http(".cache")
h.add_credentials('', '')


global_terms={}
solr_url='https://wwwwww/be'
main_filter=" PackageID =xxx "
 
 
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


def test_connection(cn, constr):
    rebuild=False
    if cn is None:
        rebuild=True
    elif not cn:
        rebuild=True
    if not rebuild:
        cn= sa.create_engine(constr)
    return cn
   
def get_translations(conn):   
    sql="with a as \
(  \
 SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1  \
WHERE   "+main_filter+"), \
c_mor  \
AS  \
 (SELECT * FROM  [TMS].[dbo].[Constituents] WHERE  ConstituentTypeID = 2 OR ConstituentTypeID=4), \
 d \
AS \
(  \
SELECT a.ID as ObjectID, c_mor.DisplayName, c_mor.BeginDate, c_mor.EndDate, Role , ConstituentTypeID, c_mor.ConstituentID \
FROM [TMS].[dbo].[vConXrefsAll]  \
INNER JOIN c_mor \
ON vConXrefsAll.ConstituentID = c_mor.ConstituentID  \
INNER JOIN a ON  \
a.[ID]=vConXrefsAll.ID  \
WHERE [vConXrefsAll].TableID = 108  \
AND vConXrefsAll.Active = 1  \
AND (RoleTypeID IN (2, 5) OR (RoleTypeID = 1 AND RoleID = 1))  \
), \
e \
AS \
(SELECT e.ConstituentID, e.DisplayName, e.AlphaSort, d.DisplayName as base_name FROM  [TMS].[dbo].[ConAltNames] e INNER JOIN d ON e.ConstituentID = d.ConstituentID) \
SELECT DISTINCT * FROM e \
"
    data=pnd.read_sql(sql=sql, con=conn)
    return data
 
def get_constituents(conn):
    sql="with a as \
(  \
 SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1  \
 WHERE   "+main_filter+"),  \
  c_phys  \
 AS  \
 (SELECT * FROM [TMS].[dbo].[Constituents] WHERE  ConstituentTypeID = 1 OR ConstituentTypeID=3), \
 c_mor  \
 AS  \
 (SELECT * FROM  [TMS].[dbo].[Constituents] WHERE  ConstituentTypeID = 2 OR ConstituentTypeID=4), \
b  \
AS  \
(  \
SELECT a.ID as ObjectID, c_phys.DisplayName, c_phys.BeginDate, c_phys.EndDate, Role , ConstituentTypeID, c_phys.ConstituentID \
FROM [TMS].[dbo].[vConXrefsAll]  \
INNER JOIN c_phys \
ON vConXrefsAll.ConstituentID = c_phys.ConstituentID  \
INNER JOIN a ON  \
a.[ID]=vConXrefsAll.ID  \
WHERE [vConXrefsAll].TableID = 108  \
AND vConXrefsAll.Active = 1  \
AND (RoleTypeID IN (2, 5) OR (RoleTypeID = 1 AND RoleID = 1))  \
AND ( endDate !=0 OR (EndDate=0 and BeginDate< 1910 and ConstituentTypeID=1) )    \
) \
, \
c \
AS \
(  \
SELECT a.ID as ObjectID, 'NOT_DISPLAYED' DisplayName, NULL BeginDate, NULL EndDate, Role , ConstituentTypeID, c_phys.ConstituentID \
FROM [TMS].[dbo].[vConXrefsAll]  \
INNER JOIN c_phys \
ON vConXrefsAll.ConstituentID = c_phys.ConstituentID  \
INNER JOIN a ON  \
a.[ID]=vConXrefsAll.ID  \
WHERE [vConXrefsAll].TableID = 108  \
AND vConXrefsAll.Active = 1  \
AND  \
(RoleTypeID IN (2, 5) OR (RoleTypeID = 1 AND RoleID = 1))  \
AND  \
NOT (  \
endDate !=0  \
OR (EndDate=0 and BeginDate< 1910 and ConstituentTypeID=1)  \
)),  \
d \
AS \
(  \
SELECT a.ID as ObjectID, c_mor.DisplayName, c_mor.BeginDate, c_mor.EndDate, Role , ConstituentTypeID, c_mor.ConstituentID \
FROM [TMS].[dbo].[vConXrefsAll]  \
INNER JOIN c_mor \
ON vConXrefsAll.ConstituentID = c_mor.ConstituentID  \
INNER JOIN a ON  \
a.[ID]=vConXrefsAll.ID  \
WHERE [vConXrefsAll].TableID = 108  \
AND vConXrefsAll.Active = 1  \
AND (RoleTypeID IN (2, 5) OR (RoleTypeID = 1 AND RoleID = 1))  \
) \
, \
e \
AS \
( \
SELECT *  FROM b  \
UNION  \
SELECT * FROM c \
UNION  \
SELECT * FROM d) \
SELECT * FROM e  "
    data=pnd.read_sql(sql=sql, con=conn)
    return data
       
 
def get_tombstone(conn):
    sql="With c as \
  ( SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1 WHERE "+ main_filter+ ")\
SELECT DISTINCT t.[ObjectID], t.[ObjectNumber] , t.[SortNumber], t.Medium, t.Dimensions , t.Title FROM \
dbo.[vgsrpObjTombstoneD_RO] t  INNER JOIN c ON c.[ID]=t.[ObjectID]  "
    data=pnd.read_sql(sql=sql, con=conn)
    return data
   
def get_sites(conn):
    sql="With c as \
  ( SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1 WHERE "+ main_filter+ ")\
SELECT DISTINCT  t.[ID] as [ObjectID], t.[SitesOfCollectionFlat] , t.[SitesOfProductionFlat]  FROM \
dbo.[vRmcaLvObjectsGeography ] t  INNER JOIN c ON c.[ID]=t.[ID]"
    data=pnd.read_sql(sql=sql, con=conn)
    return data
   
def get_acquisition_metadata(conn):
    sql="With c as \
  ( SELECT c1.* FROM   [TMS].[dbo].[PackageList] c1 WHERE "+ main_filter+ ")\
SELECT DISTINCT  t.[ID] as [ObjectID], t.[AccessionISODate] , t.[AccessionMethod]  FROM \
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
    
def sort_for_proche(obj_number):
    obj_number=obj_number.strip()
    returned=obj_number
    if len(returned)>0:
        if returned[0].isnumeric():
            returned="ZZ_"+returned
    tmp2=re.split('\-|\.', returned)
    tmp3=list(map(lambda x:str(x).rjust(6,'0'), tmp2))
    returned='.'.join(tmp3)    
    return returned
    
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
        "sort_number":prepare_json_object(sort_for_proche(objnumber)),
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
   
def add_const(doc, field_name, values):
    if len(values)>0:
        doc[field_name]= values
    return doc
   
def handle_constituents(pnd_cons, obj_id, pnd_translations):
    #print("cons_obj= "+str(obj_id))
    dict_c={}
    filtered=pnd_cons[pnd_cons["ObjectID"]==obj_id]
    const_general=[]
    const_date_general=[]
    const_collector=[]
    const_depositor=[]
    const_depot=[]
    const_donor=[]
    const_excavation_by=[]
    const_exchange=[]
    const_field_collector=[]
    const_first_owner=[]
    const_intermediary=[]
    const_legator=[]
    const_lender=[]
    const_maker_creator=[]
    const_mission=[]
    const_owner=[]
    const_previous_owner=[]
    const_transfer=[]
    const_unknown=[]
    const_vendor=[]
    
    const_date_collector=[]
    const_date_depositor=[]
    const_date_depot=[]
    const_date_donor=[]
    const_date_excavation_by=[]
    const_date_exchange=[]
    const_date_field_collector=[]
    const_date_first_owner=[]
    const_date_intermediary=[]
    const_date_legator=[]
    const_date_lender=[]
    const_date_maker_creator=[]
    const_date_mission=[]
    const_date_owner=[]
    const_date_previous_owner=[]
    const_date_transfer=[]
    const_date_unknown=[]
    const_date_vendor=[]
    
    const_trans_general=[]
    const_trans_maker_creator=[]
    trans_list=[]
    for i, row in filtered.iterrows():
        #print(row)
        
        pk=row["ConstituentID"]
        name=row["DisplayName"]
        #print(name)
        
        trans=pnd_translations[pnd_translations["base_name"]==name]
        if len(trans)>0:
            #print("translation exists")
            #print(row["DisplayName"])
            for i_trans, row_trans in trans.iterrows():
                #print(row_trans)
                if not row_trans["DisplayName"] is None and not name is None:
                    if row_trans["DisplayName"].strip() != name.strip():
                        trans_list.append(row_trans["DisplayName"].strip())
                trans_list=list(set(trans_list))
            #print(name)
            #print(trans_list)
        if not name is None:
            trans_list.append(name.strip())
        role=row["Role"]
        
        date_begin=row["BeginDate"] or 0
        date_end=row["EndDate"] or 0
        role=role.lower()
        str_year=""
        if date_begin==0 and date_end==0:
            str_year=""
        elif date_begin>0 and date_end==0:
            str_year=" ("+str(int(date_begin))+"- ???? )"
        elif date_begin==0 and date_end>0:
            str_year=" ( ???? -"+str(int(date_end))+")"
        elif date_begin>0 and date_end>0:
            str_year=" ("+str(int(date_begin))+"-"+str(int(date_end))+")"
            #print(str_year)
        if name is None:
            name=""
        if str_year is None:
            str_year=""
        name_with_date=name+str_year
        if role=="collector":
            const_collector.append(name)
            const_date_collector.append(name_with_date)
            #const_general.append(name)
        elif role=="depositor":
            const_depositor.append(name)
            const_date_depositor.append(name_with_date)
            #const_general.append(name)             
        elif role=="depot":
            const_depot.append(name)
            const_date_depot.append(name_with_date)
            #const_general.append(name)  
        elif role=="donor":
            const_donor.append(name)
            const_date_donor.append(name_with_date)
            #const_general.append(name)              
        elif role=="excavation by":
            const_excavation_by.append(name)
            const_date_excavation_by.append(name_with_date)
            #const_general.append(name)
        elif role=="exchange":
            const_exchange.append(name)
            const_date_exchange.append(name_with_date)
            #const_general.append(name)            
        elif role=="field collector":
            const_field_collector.append(name)
            const_date_collector.append(name_with_date)
            #const_general.append(name)
        elif role=="first owner":
            const_first_owner.append(name)
            const_date_first_owner.append(name_with_date)
            #const_general.append(name)
        elif role=="intermediary":
            const_intermediary.append(name)
            const_date_intermediary.append(name_with_date)
            #const_general.append(name)
        elif role=="legator":
            const_legator.append(name)
            const_date_legator.append(name_with_date)
            #const_general.append(name)
        elif role=="lender":
            const_lender.append(name)
            const_date_lender.append(name_with_date)
            #const_general.append(name)
        elif role=="maker/createur":
            const_maker_creator.append(name)
            const_date_maker_creator.append(name_with_date)
            const_trans_maker_creator.append(name)
            const_trans_maker_creator=const_trans_maker_creator+trans_list
            
            #const_general.append(name)
        elif role=="mission":
            const_mission.append(name)
            const_date_mission.append(name_with_date)
            #const_general.append(name)
        elif role=="owner":
            const_owner.append(name)
            const_date_owner.append(name_with_date)
            #const_general.append(name)
        elif role=="previous owner":
            const_previous_owner.append(name)
            const_date_previous_owner.append(name_with_date)
            #const_general.append(name)
        elif role=="transfer":
            const_transfer.append(name)
            const_date_transfer.append(name_with_date)
            #const_general.append(name)
        elif role=="unknown":
            const_unknown.append(name)
            const_date_unknown.append(name_with_date)
            #const_general.append(name)
        elif role=="vendor":
            const_vendor.append(name)
            const_date_vendor.append(name_with_date)
            #const_general.append(name)
        const_general.append(name)
        const_date_general.append(name_with_date)
        dict_c=add_const(dict_c, "const_collector", const_collector)
        dict_c=add_const(dict_c, "const_depositor",const_depositor)
        dict_c=add_const(dict_c, "const_depot",const_depot)
        dict_c=add_const(dict_c, "const_donor",const_donor)
        dict_c=add_const(dict_c, "const_excavation_by",const_excavation_by)
        dict_c=add_const(dict_c, "const_exchange",const_exchange)
        dict_c=add_const(dict_c, "const_field_collector",const_field_collector)
        dict_c=add_const(dict_c, "const_first_owner",const_first_owner)
        dict_c=add_const(dict_c, "const_intermediary",const_intermediary)
        dict_c=add_const(dict_c, "const_legator",const_legator)
        dict_c=add_const(dict_c, "const_lender",const_lender)
        dict_c=add_const(dict_c, "const_maker_creator",const_maker_creator)
        dict_c=add_const(dict_c, "const_mission",const_mission)
        dict_c=add_const(dict_c, "const_owner",const_owner)
        dict_c=add_const(dict_c, "const_previous_owner",const_previous_owner)
        dict_c=add_const(dict_c, "const_transfer",const_transfer)
        dict_c=add_const(dict_c, "const_unknown",const_unknown)
        dict_c=add_const(dict_c, "const_vendor",const_vendor)
        
        dict_c=add_const(dict_c, "const_date_collector", const_date_collector)
        dict_c=add_const(dict_c, "const_date_depositor",const_date_depositor)
        dict_c=add_const(dict_c, "const_date_depot",const_date_depot)
        dict_c=add_const(dict_c, "const_date_donor",const_date_donor)
        dict_c=add_const(dict_c, "const_date_excavation_by",const_date_excavation_by)
        dict_c=add_const(dict_c, "const_date_exchange",const_date_exchange)
        dict_c=add_const(dict_c, "const_date_field_collector",const_date_field_collector)
        dict_c=add_const(dict_c, "const_date_first_owner",const_date_first_owner)
        dict_c=add_const(dict_c, "const_date_intermediary",const_date_intermediary)
        dict_c=add_const(dict_c, "const_date_legator",const_date_legator)
        dict_c=add_const(dict_c, "const_date_lender",const_date_lender)
        dict_c=add_const(dict_c, "const_date_maker_creator",const_date_maker_creator)
        dict_c=add_const(dict_c, "const_date_mission",const_date_mission)
        dict_c=add_const(dict_c, "const_date_owner",const_date_owner)
        dict_c=add_const(dict_c, "const_date_previous_owner",const_date_previous_owner)
        dict_c=add_const(dict_c, "const_date_transfer",const_date_transfer)
        dict_c=add_const(dict_c, "const_date_unknown",const_date_unknown)
        dict_c=add_const(dict_c, "const_date_vendor",const_date_vendor)
        
        const_general=list(set(const_general))        
        dict_c=add_const(dict_c, "const_general",const_general)
        const_date_general=list(set(const_date_general))
        dict_c=add_const(dict_c, "const_date_general",const_date_general)
        
        const_trans_maker_creator=list(set(const_trans_maker_creator))    
        dict_c=add_const(dict_c, "const_trans_maker_creator",const_trans_maker_creator)
        trans_list=const_general+trans_list
        trans_list=list(set(trans_list))
        #print(trans_list)
        dict_c=add_const(dict_c, "const_trans_general",trans_list)
    return dict_c
 
cn = sa.create_engine('mssql+pyodbc://db/TMS?driver=ODBC Driver 17 for SQL Server')
#cn_thesaurus = sa.create_engine('mssql+pyodbc://db/TMSThesaurus?driver=ODBC Driver 17 for SQL Server')
 
 
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
print("Got Acquisition metadata")
print_time()
print(len(pnd_acq_metadata))
pnd_constituents=get_constituents(cn)
print("Got constituents")
print_time()
pnd_translations=get_translations(cn)
print("Got translations")
print_time()

pnd_constituents["DisplayName"]=pnd_constituents["DisplayName"].str.strip()
pnd_constituents["DisplayName"]=pnd_constituents["DisplayName"].replace(chr(160), " ")


pnd_translations["base_name"]=pnd_translations["base_name"].str.strip()
pnd_translations["base_name"]=pnd_translations["base_name"].replace(chr(160), " ")

#for index, row in pnd_translations.iterrows():
#    print(row)

cn.dispose()
 
p_all=main_data.merge(pnd_sites, on='ObjectID')
p_all=p_all.merge(pnd_acq_metadata, on='ObjectID')



for index, row in p_all.iterrows():
    try:
        #print(index)
        #print(row)
        doc=create_doc( row["ObjectID"], row["ObjectNumber"], row["SortNumber"], row["Title"], row["Medium"], row["Dimensions"] , row["SitesOfCollectionFlat"] , row["SitesOfProductionFlat"] , row["AccessionISODate"] , row["AccessionMethod"] , datetime.datetime.now().isoformat())
        list_const=handle_constituents(pnd_constituents, row["ObjectID"], pnd_translations)
        #print(list_const)
        
        #print(doc)
        #print(i)
        insert_solr(h, solr_url, doc, list_const)
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
   
