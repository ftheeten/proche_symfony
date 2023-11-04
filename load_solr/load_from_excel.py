import pandas as pnd
import traceback
import sys
import re
import httplib2
import datetime
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET

#const_general
#const_date_general

USER=""
PASSWORD=""
URL_SOLR="http://localhost:8983/solr/proche-prod/"
SRC_EXCEL="C:\\DEV\\PROCHE\\dump_solr20231102.xlsx"

FIELD_LIST=[
"creation_date",
"dimensions",
"material_and_technique",
"method_of_acquisition",
"object_number",
"site_of_collection",
"site_of_production",
"title"]
FIELD_LIST_ARRAY=["const_collector",
"const_date_collector",
"const_date_depositor",
"const_date_depot",
"const_date_donor",
"const_date_excavation_by",
"const_date_exchange",
"const_date_first_owner",
"const_date_intermediary",
"const_date_legator",
"const_date_lender",
"const_date_maker_creator",
"const_date_mission",
"const_date_owner",
"const_date_previous_owner",
"const_date_transfer",
"const_date_unknown",
"const_date_vendor",
"const_depositor",
"const_depot",
"const_donor",
"const_excavation_by",
"const_exchange",
"const_field_collector",
"const_first_owner",
"const_intermediary",
"const_legator",
"const_lender",
"const_maker_creator",
"const_mission",
"const_owner",
"const_previous_owner",
"const_trans_general",
"const_trans_maker_creator",
"const_transfer",
"const_unknown",
"const_vendor"]
FIELD_LIST_DATE=["date_of_acquisition"]
FIELD_LIST_NUM=["id", "year_of_acquisition"]
SORT_FIELD_SOURCE="object_number"
SORT_FIELD_NAME="sort_number"

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
    print(insert_url)
    print(resp)
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
    print(commit_url)
    resp2, content2= p_h.request(commit_url)
    
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
 
def read_excel(p_file, p_url_solr, p_fields, p_fields_array, p_fields_num, p_fields_date, p_sort_field_source, p_sort_field_name, p_user, p_password):
    df_src=pnd.read_excel(p_file)
    df_src.fillna("", inplace=True)
    for index, row in df_src.iterrows():
        try:
            print(index)
            print("---------------")
            doc={}
            dict_array={}
            multi_fields=[]
            for field in p_fields:
                tmp=str(row[field])
                if len(tmp)>0:
                    print(field+"\t"+tmp)
                    tmp=tmp.replace("\,",",")
                    print(field+"\t"+tmp+"\t(replace)")
                    if field == p_sort_field_source:
                        doc[p_sort_field_name]=sort_for_proche(tmp)
                    doc[field]=tmp
            for field_list in p_fields_array:
                tmp=str(row[field_list])
                if len(tmp)>0:
                    tmp_list=re.split(r"(?<![\\]),", tmp)
                    tmp_list=list(map(lambda x: x.replace('\,', ','), tmp_list))
                    print(field_list+"\t"+str(tmp_list))
                    dict_array[field_list]=tmp_list
            for  field_num in  p_fields_num:
                tmp=row[field_num]
                print(field_num+" (num)\t"+str(tmp))
                doc[field_num]=str(int(tmp))
            for  field_date in  p_fields_date:
                tmp=row[field_date]
                print(field_date+" (date)\t"+str(tmp))
                doc[field_date]=str(tmp)
            print(doc)
            h = httplib2.Http(".cache")
            if len(p_user)>0:
                h.add_credentials(p_user, p_password)
            insert_solr(h, p_url_solr, doc, dict_array)
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
        
def run():
    global SRC_EXCEL
    global URL_SOLR
    global FIELD_LIST
    global FIELD_LIST_ARRAY
    global FIELD_LIST_NUM
    global FIELD_LIST_DATE
    global SORT_FIELD_SOURCE
    global SORT_FIELD_NAME
    global USER
    global PASSWORD
    read_excel(SRC_EXCEL, URL_SOLR, FIELD_LIST, FIELD_LIST_ARRAY, FIELD_LIST_NUM, FIELD_LIST_DATE, SORT_FIELD_SOURCE,SORT_FIELD_NAME, USER, PASSWORD)
    
if __name__ == '__main__':
    sys.exit(run())