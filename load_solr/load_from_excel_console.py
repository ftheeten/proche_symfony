
import traceback
import configparser
import re
import sys
import httplib2
import datetime
import pandas as pnd
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET
import numpy as np
#import ssl

EXCEL_SRC="dump_solr20231102.xlsx"
URL_SOLR="http://127.0.0.1:8983/solr/proche-prod/"
USER_SOLR=""
PASSWORD_SOLR=""


#ctx = ssl.create_default_context()
#ctx.check_hostname = False
#ctx.verify_mode = ssl.CERT_NONE

output_excel=None
input_solr=None
input_user=None
input_password=None

field_list=[]
field_list_arrays=[]
field_list_num=[]
field_list_date=[]
sort_field_source=None
sort_field_name=None

def print_time():
    now = datetime.datetime.now()
    print ("Current date and time : ")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))

def insert_solr(p_h, p_solr_url,  p_fields, list_multi_fields=None ):
    global ctx
    list_fields=[]
    insert_url=p_solr_url+"update"
    commit_url=p_solr_url+"update?commit=true"
    content=""
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
    #print(insert_url)
    #print(resp)
    #print(content)
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
    #print(commit_url)
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
    print("Excel read, beginning importation")
    print_time()
    #print(p_user)
    #print(p_password)
    #print(p_sort_field_source)
    df_src=pnd.read_excel(p_file)
    df_src = df_src.replace({np.nan: None})
    df_src=df_src.astype(str)
    df_src.fillna("", inplace=True)
    #df_src= df_src.astype(str)
    h = httplib2.Http(disable_ssl_certificate_validation=True)
    if len(p_user)>0:
        #print(p_user)
        #print(p_password)
        h.add_credentials(p_user, p_password)
    for index, row in df_src.iterrows():
        try:
            #print(index)
            #print("---------------")
            doc={}
            dict_array={}
            multi_fields=[]
            for field in p_fields:
                tmp=str(row[field])
                if len(tmp)>0:
                    #print(field+"\t"+tmp)
                    tmp=tmp.replace("\,",",")
                    #print(field+"\t"+tmp+"\t(replace)")
                    if field == p_sort_field_source:
                        #print("sort")
                        doc[p_sort_field_name]=sort_for_proche(tmp)
                    doc[field]=tmp
            for field_list in p_fields_array:
                tmp=str(row[field_list])
                if len(tmp)>0:
                    tmp_list=re.split(r"(?<![\\]),", tmp)
                    tmp_list=list(map(lambda x: x.replace('\,', ','), tmp_list))
                    #print(field_list+"\t"+str(tmp_list))
                    dict_array[field_list]=tmp_list
            for  field_num in  p_fields_num:
                tmp=row[field_num]
                #print(field_num+" (num)\t"+str(tmp))
                if len(str(tmp)) > 0:
                    doc[field_num]=str(int(tmp))
                    if field_num !="id":
                        doc[field_num+"_str"]=str(int(tmp))
            for  field_date in  p_fields_date:
                tmp=row[field_date]
                #print(field_date+" (date)\t"+str(tmp))
                doc[field_date]=str(tmp)
            #print(doc)
            
            insert_solr(h, p_url_solr, doc, dict_array)
            if index%100==0 or index==0:
                print(index)
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
            print("data:")
            print(row)
            sys.exit(1)
        except KeyboardInterrupt as ex:
            sys.exit()
    print("Done")
    print(str(index-1)+ " record(s) imported")
    print_time()
    
def import_in_solr():
    global URL_SOLR
    global USER_SOLR
    global PASSWORD_SOLR
    global EXCEL_SRC
    global field_list
    global field_list_arrays
    global field_list_num
    global field_list_date
    global sort_field_source
    global sort_field_name
    
    global input_user
    global input_password

    print("Loading Excel, please wait")
    print_time()
    read_excel(EXCEL_SRC, URL_SOLR, field_list, field_list_arrays, field_list_num, field_list_date, sort_field_source,sort_field_name, USER_SOLR, PASSWORD_SOLR)



    

if __name__ == '__main__':
    import_in_solr()
