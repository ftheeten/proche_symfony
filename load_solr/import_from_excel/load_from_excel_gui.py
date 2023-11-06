from PySide6.QtWidgets import QApplication, QWidget, QPushButton, QVBoxLayout , QFileDialog, QButtonGroup, QRadioButton, QLineEdit, QLabel, QCheckBox, QMessageBox, QComboBox, QSizePolicy
from PySide6.QtCore import Qt
import traceback
import configparser
import re
import sys
import httplib2
import datetime
import pandas as pnd
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET



CONFIG_FILE="config_proche.cfg"
URL_SOLR=""
USER_SOLR=""
PASSWORD_SOLR=""
app=None
window=None
layout=None

excel_src=None
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
    df_src=pnd.read_excel(p_file)
    df_src.fillna("", inplace=True)
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
            h = httplib2.Http(".cache")
            if len(p_user)>0:
                h.add_credentials(p_user, p_password)
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
    global excel_src
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
    if len(URL_SOLR)>0 and len(excel_src)>0:
        if len(input_user.text())>0 and len(input_password.text())>0:
            USER_SOLR=input_user.text()
            PASSWORD_SOLR=input_password.text()
        read_excel(excel_src, URL_SOLR, field_list, field_list_arrays, field_list_num, field_list_date, sort_field_source,sort_field_name, USER_SOLR, PASSWORD_SOLR)
    else:
        print("Can't load into SOLR : source Excel file and/or SOLr endpoint missing ")

def explode_conf(p_str):
    p_str=re.sub(r"\[|\]|\s+|\"|","", p_str )
    tmp=p_str.split(",")
    tmp=list(map(lambda x: x.strip(), tmp))
    return tmp
 
def choose_excel():
    global excel_src
    global output_excel
    file_name = QFileDialog()
    filter = "xlsx (*.xlsx);;others (*.*)"
    excel_src, _= file_name.getOpenFileName(window, "Open files", "", filter)
    output_excel.setText(excel_src)
    
def start():
    global app
    global window
    global layout
    global output_excel
    global input_solr
    global input_user
    global input_password
    global URL_SOLR
    global USER_SOLR
    global PASSWORD_SOLR
    
    global CONFIG_FILE
    
    global field_list
    global field_list_arrays
    global field_list_num
    global field_list_date
    global sort_field_source
    global sort_field_name
    
  
    
    try:
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE, encoding="utf-8")
        url_solr_tmp=config["PARAMS"]["solr_url"]
        user_solr_tmp=config["PARAMS"]["solr_user"]
        password_solr_tmp=config["PARAMS"]["solr_password"]
        field_list_list=config["PARAMS"]["field_list"]
        field_list_arrays_list=config["PARAMS"]["field_list_arrays"]
        field_list_date_list=config["PARAMS"]["field_list_date"]
        field_list_num_list=config["PARAMS"]["field_list_num"]
        sort_field_source=config["PARAMS"]["sort_field_source"]
        sort_field_name=config["PARAMS"]["sort_field_name"]

        field_list=explode_conf(field_list_list)
        field_list_arrays=explode_conf(field_list_arrays_list)
        field_list_date=explode_conf(field_list_date_list)
        field_list_num=explode_conf(field_list_num_list)

    
        app = QApplication([])
        window = QWidget()
        window.setMinimumWidth(700)
        layout = QVBoxLayout()
        
        but_load_excel=QPushButton('Load file')
        layout.addWidget(but_load_excel)
        but_load_excel.clicked.connect(choose_excel)
        
        output_excel=QLabel()
        layout.addWidget(output_excel)
        
        output_solr=QLabel()
        output_solr.setText("URL SOLR:")
        layout.addWidget(output_solr)
        
        input_solr=QLineEdit()
        if len(url_solr_tmp.strip())>0:
            URL_SOLR=url_solr_tmp.replace("'","").replace("\"","").strip()
            input_solr.setText(URL_SOLR)
        layout.addWidget(input_solr)
        
        output_user=QLabel()
        output_user.setText("SOLR User")
        layout.addWidget(output_user)
        
        input_user=QLineEdit()
        if len(user_solr_tmp.strip())>0:
            USER_SOLR=user_solr_tmp.replace("'","").replace("\"","").strip()
            input_user.setText(USER_SOLR)
        layout.addWidget(input_user)
        
        
        output_password=QLabel()
        output_password.setText("SOLR Password")
        layout.addWidget(output_password)
        
        input_password=QLineEdit()
        input_password.setEchoMode(QLineEdit.EchoMode.Password)
        if len(password_solr_tmp.strip())>0:
            PASSWORD_SOLR=password_solr_tmp.replace("'","").replace("\"","").strip()
            input_password.setText(PASSWORD_SOLR)
        layout.addWidget(input_password)
        
        but_load_solr=QPushButton('Import in Solr')
        layout.addWidget(but_load_solr)
        but_load_solr.clicked.connect(import_in_solr)
        
        window.setLayout(layout)
        window.setWindowFlags(Qt.WindowType.WindowStaysOnTopHint)
        window.show()           
        app.exec()
    except:    
        traceback.print_exception(*sys.exc_info())
    

if __name__ == '__main__':
    start()