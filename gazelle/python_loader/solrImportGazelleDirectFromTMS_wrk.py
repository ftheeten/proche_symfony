import sqlalchemy as sa
import urllib
import pysolr
import traceback
import pandas as pnd
import numpy as np
import datetime
import os
import requests
import math
import sys
import logging
import cv2
from pathlib import Path
import re
import gc

#all_objectids_list = []
I_IMAGE=0
I_NEW_IMAGE=0
global_db=None
global_parents={}
SIZE=1000
LOG_FILE='/log/xxx.txt'
LOGGER=None
COPY_IMG=True

SOLR_ACCESS_POINT="https://gazelle"
global_check_exists='/'
global_storage_backup={
# images 'new' system
                'newImageHandlingImageServer' : "",
                'newImageHandlingImagePrefix' : "",


                # images 'old' system
#                'fullsizeprefix' => "",
                'primaryImagePrefix': "",
                'thumbnailprefix' : "",
                'mountdir' : "",

#                'iipimageviewerurl' => "",
#                'seadragon_loris_url' =>"",
#                'seadragon_loris_url' => "",

                'gazelleImagesWebserver' : "",

                'imagePathsThumbs' : {
                    '/1' : "",
                    '/2' : "",
                    '/3' : "",
                    '/4' : "",
                },
                'imagePathsLarge' : {
                     '/1' : "",
                    '/2' : "",
                    '/3' : "",
                    '/4' : "",
                 },
                'imagePathsOriginal' :{
                     '/1' : "",
                    '/2' : "",
                    '/3' : "",
                    '/4' : "",
                },
                'gazelle_tifs_prefix' : "",
#              'gazelle_tifs_orig_prefix' => "",
                'gazelle_tifs_orig_prefix' : "",
}

global_storage={
                # images 'new' system -- okapi
                # https://wwww.test.com
                # http://172.16.2.253

                # okapi
#                'newImageHandlingImageServer' :"",
#                'newImageHandlingImagePrefix' : "",
                'okapi_image_server' :"",
                'okapi_image_server_prefix' : "",
                'okapi_fullsize_basedir' : "",

               # images 'old' system -- eo-fiches
#                'fullsizeprefix' : "",
#                'primaryImagePrefix' : "",
#                'thumbnailprefix' :"",
                'primaryImagePrefix' :"",
                'thumbnailprefix' : "",
                'mountdir' :"",
                'eofiches_primary_image_prefix' : "",
                'eofiches_thumbnail_prefix' :"",
                'eofiches_fullsize_basedir' : "",

#                'iipimageviewerurl' : "",
#                'seadragon_loris_url' : "",
#                'seadragon_loris_url' : "",
#                'seadragon_loris_url' : "",


                'gazelleImagesWebserver' : "https://wwww.test.com/collections/",
                'imagePathsThumbsNAS' : {
                    
                    '///tms/' : "",
                },
                'imagePathsThumbs' : {
                     '/1' : "",
                    '/2' : "",
                    '/3' : "",
                    '/4' : "",
                },
                'imagePathsLarge' : {
                     '/1' : "",
                    '/2' : "",
                    '/3' : "",
                    '/4' : "",
                 },
                'imagePathsOriginal' : {
                      '/1' : "",
                    '/2' : "",
                    '/3' : "",
                    '/4' : "",
                 },
                'gazelle_tifs_prefix' : "",
                'gazelle_tifs_orig_prefix' : "",
           }
#IMG_SRC=["/mnt///FICHE/EO", "/mnt///DIA" , "/mnt///WORK"]

IMG_SRC=["./FICHE", "./DIA" , "./WORK" , "./PHOTO", "./PRO", "./SCAN", "./WEB", "./NEG" ]
IMG_TARGET={
    "600px":"600px//",
    "thumbs":"/"
}

IMG_PREFIX_ORIGINAL="/server_folder/"
IMG_PREFIX_CHECK="./"
IMG_MAPPING_SECURITY={
        "/":"/"
    }
    

THRESHOLD_GEO_KEYWORD=6
MAX_SIZE_BIG=600
MAX_FILE_THUMB=150

url_iif="https://ca.museum.africamuseum.be/collections/index.php/sd?file="
url_download="https://ca.museum.africamuseum.be/collections/index.php/downloadtmsimage/"


global_where="WHERE ObjectID > 0"




mapping_cons={
23:"cons_afgebeeld",
29:"cons_bruikleen",
27:"cons_depot",
21:"cons_eigenaar",
30:"cons_erflater",
34:"cons_exchange",
24:"cons_fotograaf",
41:"cons_graveerder",
20:"cons_identificatie",
1:"cons_maker",
19:"cons_missie",
31:"cons_onbekend",
39:"cons_oorspronkelijke_eigenaar",
26:"cons_opdrachtgever",
2:"cons_schenker",
25:"cons_tussenpersoon",
35:"cons_tussenpersoon",
40:"cons_uitgever",
18:"cons_veldverzamelaar",
16:"cons_verkoper",
5:"cons_vorige_eigenaar",
84:"cons_excol_collector",
100:"cons_excol_depositor",
112:"cons_excol_depot",
85:"cons_excol_donor",
2:"cons_excol_donor",
108:"cons_excol_excavation_by",
86:"cons_excol_exchange",
96:"cons_excol_field_collector",
87:"cons_excol_intermediary",
88:"cons_excol_legator",
91:"cons_excol_owner",
109:"cons_excol_prospection_by",
92:"cons_excol_transfer",
93:"cons_excol_unknown",
94:"cons_excol_vendor",
89:"cons_excol_lender",
90:"cons_excol_mission",
}



mapping_titles={0: "objtitle_niet_ingevoerd",
        4: "objtitle_wetenschappelijk",
        5: "objtitle_originele_titel",
        6: "objtitle_vernaculaire_naam",
        7: "objtitle_werktitel",
        8: "objtitle_brief_description_temp",
        9: "objtitle_publiekstitel",
        10: "objtitle_legacy_title"}
        
mapping_relationships={
    2: "inseparable objects",
    7: "See also",
    11: "double"}
    
pattern=re.compile('(\d{1,2})(-|/)(\d{1,2})(-|/)(\d{4})')
pattern2=re.compile('(.*?)(\d{4})(.*?)')
pattern3=re.compile('(.*?)(\d{4})(.*)(\d{4})(.*?)')


def print_time():
    now = datetime.datetime.now()
    print ("Current date and time : ")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))


def getDBConnection():
    print("connect TMS")
    params = urllib.parse.quote_plus(r'Driver={ODBC Driver 18 for SQL Server};Server=db,1433;Database=TMS;Uid=tmswebservices;Pwd=tmswebservices$;TrustServerCertificate=yes;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    cn = sa.create_engine(conn_str)
    return cn


def clean_str(p_str):
    if p_str is None:
        return ""
    elif p_str.lower().strip()=="null":
        return ""
    else:
        return p_str.strip()
        
def getAllObjectIdsToDo():
    global all_objectids_list
    global global_db
    global global_where
    sql="SELECT DISTINCT   ObjectID as ctrl_id FROM Objects "+global_where+ " ORDER BY ObjectID"
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
 
#def get_packages(): not displayed

def get_obj_description():
    global global_db
    sql="with a as\
(SELECT  DISTINCT Objects.ObjectID, Objects.ObjectNumber,\
    ObjContext.ShortText8 as brief_description,   ObjAccession.AccessionISODate,  AccessionMethods.AccessionMethod,\
    Departments.Department,Objects.Dimensions, ObjContext.ShortText9 as Title,\
    Objects.Exhibitions,Objects.PubReferences, COALESCE(NULLIF(LTRIM(RTRIM(Objects.[SortNumber])),''),'ZZ_UNK') SortNumber, Objects.Description,\
    Objects.DateRemarks\
    , Objects.Medium as Material,\
    CASE WHEN COALESCE( RTRIM(LTRIM(Objects.DateRemarks)),'')='' THEN\
    Objects.Dated\
    ELSE\
    ''\
    END\
    as date_of_production,\
    Dated, \
    CASE WHEN COALESCE( RTRIM(LTRIM(Objects.DateRemarks)),'')='' THEN\
    COALESCE(NULLIF(ObjContext.ShortText6,''),ObjDates.DateText)\
    ELSE\
    NULL\
    END\
    as date_of_collection\
    FROM Objects INNER JOIN ObjContext ON Objects.ObjectID = ObjContext.ObjectID\
    INNER JOIN ObjAccession ON Objects.ObjectID = ObjAccession.ObjectID\
               INNER JOIN AccessionMethods ON ObjAccession.AccessionMethodID = AccessionMethods.AccessionMethodID\
    INNER JOIN AuthorityValues ON ObjContext.Authority2ID = AuthorityValues.AuthorityID\
    INNER JOIN Departments ON Objects.DepartmentID = Departments.DepartmentID\
    LEFT JOIN ObjDates ON Objects.ObjectID=ObjDates.ObjectID AND  EventType = 'Date of collecting'),\
    c as\
    (SELECT  a.ObjectID, DateText as date_of_previous_acquisition, Remarks date_of_previous_acquisition_remarks FROM ObjDates inner join a on a.ObjectID =ObjDates.ObjectID\
    WHERE  lower(EventType) = 'previous acquisition' ),\
    d as (\
               SELECT MediaXrefs.ID as med_obj_id, MediaFiles.FileName as iiif_manifest\
                        FROM MediaXrefs INNER JOIN MediaMaster INNER JOIN ThesXrefs\
                        ON MediaMaster.MediaMasterID = ThesXrefs.ID ON MediaXrefs.MediaMasterID = MediaMaster.MediaMasterID\
                                                                                          INNER JOIN MediaRenditions ON MediaMaster.MediaMasterID = MediaRenditions.MediaMasterID\
                                                                                          INNER JOIN MediaFiles ON MediaRenditions.RenditionID = MediaFiles.RenditionID\
                        WHERE ThesXrefs.ThesXrefTypeID=66 AND ThesXrefs.TableID=318 AND ThesXrefs.TermID=1752859\
                        AND ThesXrefs.ThesXrefTableID=343 AND MediaMaster.DepartmentID=95\
               )\
    select distinct a.* , COALESCE(date_of_previous_acquisition,'') date_of_previous_acquisition, COALESCE(date_of_previous_acquisition_remarks,'') date_of_previous_acquisition_remarks, iiif_manifest from a LEFT JOIN c ON a.ObjectID=c.ObjectID\
               LEFT JOIN d ON a.ObjectID=d.med_obj_id"
    data=pnd.read_sql(sql=sql, con=global_db)
    data["date_of_previous_acquisition_final"] = data['date_of_previous_acquisition'].astype(str) +" "+ data["date_of_previous_acquisition_remarks"]
    data=data.map(lambda x: x.strip() if isinstance(x, str) else x)
    #print(data)
    #print(data.columns.tolist())
    data = data.replace({np.nan: ''})
    data["date_of_previous_acquisition_final_concat"]=""
    tmp = data.groupby(['ObjectID'], as_index=False)[['date_of_previous_acquisition_final']].agg(lambda x: ', '.join(map(str, set(x))))
  
    
    tmp = data.groupby(['ObjectID'], as_index=False)[['date_of_previous_acquisition_final']].agg(lambda x: ' - '.join(map(str, set(x))))
    #print(tmp)
    #print("-------------")
    tmp['date_of_previous_acquisition_final'] = tmp['date_of_previous_acquisition_final'].astype('str')
    tmp=tmp.map(lambda x: x.strip() if isinstance(x, str) else x)
    #print(tmp[tmp.date_of_previous_acquisition_final.str.len()>0])
    #print(tmp[tmp["ObjectID"]==95859])
    debug=tmp[tmp["date_of_previous_acquisition_final"].str.len()>0]
    for i, row in debug.iterrows():
        #print(i)
        #print(row["date_of_previous_acquisition_final"])
        data.loc[data["ObjectID"]==row["ObjectID"],"date_of_previous_acquisition_final_concat"]=row["date_of_previous_acquisition_final"]
    data=data.drop_duplicates()
    return data

#41,31, 28 (material)
def  get_thesaurus():
    global global_db
    sql="SELECT ThesXrefs.ID, ThesXrefs.TermID, ThesXrefs.ThesXrefTypeID, Term  FROM [TMS].[dbo].ThesXrefs LEFT JOIN [TMSThesaurus].[dbo].terms\
    ON  ThesXrefs.TermID=terms.TermID\
    WHERE ThesXrefs.TableID=108 AND ThesXrefs.Active <> 0"
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
    
def  get_collection_place():
    global global_db
    sql="SELECT objectid,  longtext7 as field_collection_place,  longtext8 as production_place FROM [TMS].[dbo].ObjContext "
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    


def create_key_word_string(pnd_keyword):
    pnd_keyword=pnd_keyword.sort_values(['base_term_id', 'CN'], ascending=[True, True])
    base_term_list=pnd_keyword["base_term_id"].unique()
    returned = pnd.DataFrame(columns=["base_term_id", "keyword_str"])
    for term in base_term_list:
        filtered=pnd_keyword[pnd_keyword["base_term_id"]==term]
        list_terms=[]
        for i, row in filtered.iterrows():
            list_terms.append(row["Term"])
        if len(list_terms)>1:
            list_terms=list_terms[1:]
        term_str=' - '.join(list_terms)
        returned.loc[len(returned)]={"base_term_id":term, "keyword_str":term_str }
    return returned
    
def create_key_word_string_syno(pnd_keyword):
    pnd_keyword=pnd_keyword.sort_values(['base_term_id', 'CN'], ascending=[True, True])
    base_term_list=pnd_keyword["base_term_id"].unique()
    returned = pnd.DataFrame(columns=["base_term_id", "keyword_str","list_syno","base_level"])
    pnd_tmp=pnd_keyword[['base_term_id', 'base_level']]
    pnd_tmp=pnd_tmp.groupby('base_term_id').agg(base_level=('base_level', 'min'))
    #print(pnd_tmp)
    tmp_dict=pnd_tmp.to_dict()
    tmp_dict=tmp_dict["base_level"]
    #print(tmp_dict)
    del pnd_tmp
    gc.collect()
    for term in base_term_list:
        filtered=pnd_keyword[pnd_keyword["base_term_id"]==term]
        list_terms=[]
        list_syno=[]
        base_level=tmp_dict[term]
        for i, row in filtered.iterrows():
            list_terms.append(row["Term"])
            list_syno=list_syno+row["v_alt_terms"]
        if len(list_terms)>1:
            list_terms=list_terms[1:]
        term_str=' - '.join(list_terms)
        list_syno=list(set(list_syno))
        returned.loc[len(returned)]={"base_term_id":term, "keyword_str":term_str , "list_syno":list_syno, "base_level":base_level}
    return returned
 


    
def get_thesaurus_hierarchy():
    global global_db
    global global_where
    sql="WITH a0 AS\
    (SELECT ObjectID FROM [TMS].[dbo].[Objects]  "+global_where+")\
    ,a AS\
    ( SELECT distinct ThesXrefs.TermID, ThesXrefs.ThesXrefTypeID \
    FROM  [TMS].[dbo].ThesXrefs INNER JOIN a0 ON ThesXrefs.ID= a0.ObjectID WHERE ThesXrefs.TableID=108 \
    UNION SELECT distinct ColThesXrefs.TermID, ColThesXrefs.ColThesXrefTypeID FROM  [TMS].[dbo].ColThesXrefs INNER JOIN a0 ON ColThesXrefs.ID= a0.ObjectID WHERE ColThesXrefs.TableID=108 )\
    , \
    b as \
    ( SELECT distinct Terms.TermID, Terms.TermID as base_term_id, Terms.Term, Terms.TermMasterID, CN,\
    CASE WHEN CHARINDEX('.',[TermMaster].CN )>0 THEN reverse(substring(REVERSE([TermMaster].CN),  charindex('.', REVERSE([TermMaster].CN))+1,1000))   ELSE NULL END  parent\
    ,  len(CN)-len( replace(CN, '.','')) as level ,\
               Terms.LanguageID\
    FROM [TMSThesaurus].[dbo].[Terms] \
    INNER JOIN a on [Terms].TermID=a.TermID \
    LEFT JOIN [TMSThesaurus].[dbo].[TermMaster] ON [Terms].TermMasterID=[TermMaster].TermMasterID \
    UNION ALL \
    SELECT NULL, b.base_term_id, NULL,  [TermMaster].TermMasterID, [TermMaster].CN, \
    CASE WHEN CHARINDEX('.',[TermMaster].CN )>0 THEN reverse(substring(REVERSE([TermMaster].CN),  charindex('.', REVERSE([TermMaster].CN))+1,1000))  ELSE NULL  END  parent\
    , len([TermMaster].CN)-len( replace([TermMaster].CN, '.','')) as level  ,\
               NULL\
    FROM [TMSThesaurus].[dbo].[TermMaster] \
    INNER JOIN b ON [TermMaster].CN=b.parent \
   \
    ),\
               c as\
               (\
    select distinct b.*, Terms.TermID as valid_term_id,\
               Terms.Term as valid_term,\
               Terms.LanguageID as valid_term_language,\
               ROW_NUMBER() over ( partition by base_term_id, Terms.TermMasterID ORDER BY Terms.LanguageID, Terms.TermID ) as pref\
               from b \
               LEFT JOIN [TMSThesaurus].[dbo].Terms ON b.TermMasterID=Terms.TermMasterID\
               where b.termID is NULL\
               ),\
               d as\
               (\
               select *, 1 as pref from b where TermID is not null\
               union \
               select \
               valid_term_id, base_term_id,valid_term,  TermMasterID, CN, parent,level,valid_term_language, pref\
               from c WHERE pref=1 \
               )\
               select * from d "
    data=pnd.read_sql(sql=sql, con=global_db)
    returned=create_key_word_string(data)
    #print(returned)
    return returned
    
def get_thesaurus_hierarchy_for_syno():
    global global_db
    global global_where
    sql="WITH a0 AS\
    (SELECT ObjectID FROM [TMS].[dbo].[Objects]  "+global_where+")\
    ,a AS\
    ( SELECT distinct ThesXrefs.TermID, ThesXrefs.ThesXrefTypeID \
    FROM  [TMS].[dbo].ThesXrefs INNER JOIN a0 ON ThesXrefs.ID= a0.ObjectID WHERE ThesXrefs.TableID=108 \
    UNION SELECT distinct ColThesXrefs.TermID, ColThesXrefs.ColThesXrefTypeID FROM  [TMS].[dbo].ColThesXrefs INNER JOIN a0 ON ColThesXrefs.ID= a0.ObjectID WHERE ColThesXrefs.TableID=108 )\
    , \
    b as \
    ( SELECT distinct Terms.TermID, Terms.TermID as base_term_id, Terms.Term, Terms.TermMasterID, CN,\
    CASE WHEN CHARINDEX('.',[TermMaster].CN )>0 THEN reverse(substring(REVERSE([TermMaster].CN),  charindex('.', REVERSE([TermMaster].CN))+1,1000))   ELSE NULL END  parent\
    ,  len(CN)-len( replace(CN, '.','')) as level ,\
               Terms.LanguageID\
    FROM [TMSThesaurus].[dbo].[Terms] \
    INNER JOIN a on [Terms].TermID=a.TermID \
    LEFT JOIN [TMSThesaurus].[dbo].[TermMaster] ON [Terms].TermMasterID=[TermMaster].TermMasterID \
    UNION ALL \
    SELECT NULL, b.base_term_id, NULL,  [TermMaster].TermMasterID, [TermMaster].CN, \
    CASE WHEN CHARINDEX('.',[TermMaster].CN )>0 THEN reverse(substring(REVERSE([TermMaster].CN),  charindex('.', REVERSE([TermMaster].CN))+1,1000))  ELSE NULL  END  parent\
    , len([TermMaster].CN)-len( replace([TermMaster].CN, '.','')) as level  ,\
               NULL\
    FROM [TMSThesaurus].[dbo].[TermMaster] \
    INNER JOIN b ON [TermMaster].CN=b.parent \
   \
    ),\
               c as\
               (\
    select distinct b.*, Terms.TermID as valid_term_id,\
               Terms.Term as valid_term,\
               Terms.LanguageID as valid_term_language,\
               ROW_NUMBER() over ( partition by base_term_id, Terms.TermMasterID ORDER BY Terms.LanguageID, Terms.TermID ) as pref\
               from b \
               LEFT JOIN [TMSThesaurus].[dbo].Terms ON b.TermMasterID=Terms.TermMasterID\
               where b.termID is NULL\
               ),\
               d as\
               (\
               select *, 1 as pref from b where TermID is not null\
               union \
               select \
               valid_term_id, base_term_id,valid_term,  TermMasterID, CN, parent,level,valid_term_language, pref\
               from c WHERE pref=1 \
               )\
               select * from d "
    data=pnd.read_sql(sql=sql, con=global_db)
    
    return data
    

def get_terms():
    global global_db
    global global_where
    sql="WITH a0 AS \
    (SELECT  DISTINCT ObjectID as ctrl_id FROM [TMS].[dbo].[Objects] "+ global_where +"),\
    a AS  \
(\
SELECT distinct ThesXrefs.TermID, ThesXrefs.ThesXrefTypeID as type_id,ThesXrefs.ID as object_id, 'ThesXrefs'  as main_table FROM  [TMS].[dbo].ThesXrefs WHERE ThesXrefs.TableID=108 \
UNION \
SELECT distinct ColThesXrefs.TermID, ColThesXrefs.ColThesXrefTypeID, ColThesXrefs.ID, 'ColThesXrefs'  FROM  [TMS].[dbo].ColThesXrefs WHERE ColThesXrefs.TableID=108 \
), \
b as (\
select DISTINCT a.*, TermMasterID, RTRIM(LTRIM(Term)) Term from a INNER JOIN a0 ON a.object_id=a0.ctrl_id INNER JOIN [TMSThesaurus].[dbo].[Terms] ON a.TermID=[Terms].TermID  \
union \
SELECT NULL, 44, ID, 'ThesXrefs', NULL, RTRIM(LTRIM(Culture)) FROM [TMS].[dbo].[vRmcaLvObjectsCultures] c INNER JOIN a0 ON c.[ID]=a0.ctrl_id    \
union \
SELECT NULL, 44, ID, 'ThesXrefs', NULL, RTRIM(LTRIM(CulturesFlat)) FROM [TMS].[dbo].[vRmcaLvObjectsCultures] c INNER JOIN a0 ON c.[ID]=a0.ctrl_id   \
union \
SELECT NULL, 44, ID, 'ThesXrefs', NULL, RTRIM(LTRIM(CultureList)) FROM [TMS].[dbo].[vRmcaLvObjectsCultures] c INNER JOIN a0 ON c.[ID]=a0.ctrl_id  \
union \
SELECT NULL, 49, ID, 'ThesXrefs', NULL, RTRIM(LTRIM(CulturesOfProductionFlat)) FROM [TMS].[dbo].[vRmcaLvObjectsCultures] c INNER JOIN a0 ON c.[ID]=a0.ctrl_id   \
) \
SELECT * FROM b   "

    data=pnd.read_sql(sql=sql, con=global_db)
    return data
  
def get_terms_alt_names():
    global global_db
    sql="WITH a AS (SELECT  \
[TermID] alt_term_id ,\
[TermMasterID] alt_master_id ,\
a.[TermTypeID] alt_type_id,b.[TermType] alt_term_type,\
[Term] alt_term \
FROM [TMSThesaurus].[dbo].[Terms] a LEFT JOIN [TMSThesaurus].[dbo].[TermTypes] b ON a.[TermTypeID]=b.[TermTypeID] \
WHERE a.[TermTypeID]=4), \
b AS ( \
SELECT [TermID] as fk_term_id, [TermMasterID], [Term] \
FROM [TMSThesaurus].[dbo].[Terms] a LEFT JOIN [TMSThesaurus].[dbo].[TermTypes] b ON a.[TermTypeID]=b.[TermTypeID] \
) \
, c \
as \
( \
SELECT * FROM b INNER JOIN a ON b.TermMasterID=a.alt_master_id WHERE  fk_term_id!=alt_term_id AND Term !=alt_term), \
d \
as \
(select alt_term_Id, termmasterid, alt_term, fk_term_id, alt_master_id , alt_type_id, alt_Term_type, term FROM b INNER JOIN a ON \
 b.TermMasterID=a.alt_master_id WHERE  fk_term_id!=alt_term_id AND Term !=alt_term) \
select DISTINCT * from (select * from c UNION select * from d) e" 
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
def merge_terms_alt_names(p_data, p_terms, p_terms_hierarch):
    
    p_data=p_data.sort_values(by='alt_term_id',ascending=True)
    p_data=p_data.loc[p_data['alt_term'].notna()]
    p_data['alt_term']= p_data['alt_term'].str.replace(r'\s+', ' ', regex=True)
    p_data=p_data.loc[p_data['Term']!=p_data['alt_term']]
    p_data=p_data[["fk_term_id", "alt_master_id","alt_term"]]
    p_data=p_data.drop_duplicates()
    p_data=p_data.groupby(["fk_term_id"])["alt_term"].apply(list).reset_index(name='v_alt_terms')
    p_terms=pnd.merge(p_terms,p_data, how='left', left_on="TermID", right_on="fk_term_id", suffixes=('_y', '') )
    #print(p_terms_hierarch)
    p_terms_hierarch=pnd.merge(p_terms_hierarch,p_data, how='left', left_on="TermID", right_on="fk_term_id", suffixes=('_y', '') )
    #p_terms['TermMasterID'] = p_terms['TermMasterID'].astype(int)
    p_terms['fk_term_id'] = p_terms['fk_term_id'].replace(np.nan, -1)
    p_terms['fk_term_id'] = p_terms['fk_term_id'].astype(int)
    p_terms['v_alt_terms'] = p_terms['v_alt_terms'].apply(lambda d: d if isinstance(d, list) else [])
    #p_terms['v_alt_terms']= p_terms['v_alt_terms'].map(lambda l:  [x for x in l if x not in GEO_ALT_NAME_EXCEPTIONS] )
    p_terms_hierarch['fk_term_id'] = p_terms_hierarch['fk_term_id'].replace(np.nan, -1)
    p_terms_hierarch['fk_term_id'] = p_terms_hierarch['fk_term_id'].astype(int)
    p_terms_hierarch['v_alt_terms'] = p_terms_hierarch['v_alt_terms'].apply(lambda d: d if isinstance(d, list) else [])
    #p_terms_hierarch['v_alt_terms']= p_terms_hierarch['v_alt_terms'].map(lambda l:  [x for x in l if x not in GEO_ALT_NAME_EXCEPTIONS] )
    
    p_terms_hierarch_level=p_terms_hierarch[["TermID", "level"]]
    p_terms_hierarch_level=p_terms_hierarch_level.rename(columns={'level':"base_level"})
    p_terms_hierarch_level=p_terms_hierarch_level.groupby('TermID').agg(base_level=('base_level', 'min'))
    p_terms_hierarch=pnd.merge(p_terms_hierarch,p_terms_hierarch_level, how='left', left_on="base_term_id", right_on="TermID", suffixes=('_y', '') )
    p_terms_hierarch=p_terms_hierarch.drop(p_terms_hierarch.filter(regex='_y$').columns, axis=1)
    
    #print(p_terms_hierarch_level)
    del p_terms_hierarch_level
    gc.collect()
    p_terms_hierarch['base_level'] = p_terms_hierarch['base_level'].replace(np.nan, -1)
    p_terms_hierarch['base_level'] = p_terms_hierarch['base_level'].astype(int)
    #print(p_terms)
    return p_terms, p_terms_hierarch   
    
  
def get_alt_nums():
    global global_db
    sql="SELECT Description + ' - '+ AltNum as alt_num_str, id as object_id  FROM [TMS].[dbo].AltNums WHERE TableID=108"
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
def get_images():
    global global_db
    global global_where
    sql="with a0 as\
    (SELECT  DISTINCT ObjectID as ctrl_id FROM Objects "+global_where+"),\
                              b as (\
SELECT MediaFiles.RenditionID, MediaFiles.FileName,\
REPLACe(MediaPaths.PhysicalPath, '\\medias\collections\TMS\','\\MEDIAS\collections\TMS\' ) \
PhysicalPath , MediaXrefs.PrimaryDisplay, MediaFiles.FileID , Objects.ObjectID \
object_id \
FROM MediaXrefs INNER JOIN MediaMaster ON MediaXrefs.MediaMasterID = MediaMaster.MediaMasterID \
INNER JOIN MediaRenditions ON MediaMaster.MediaMasterID = MediaRenditions.MediaMasterID \
INNER JOIN MediaFiles ON MediaRenditions.RenditionID = MediaFiles.RenditionID \
INNER JOIN Objects ON MediaXrefs.ID = Objects.ObjectID \
INNER JOIN a0 ON  Objects.ObjectID =a0.ctrl_id \
INNER JOIN MediaPaths ON MediaFiles.PathID = MediaPaths.PathID \
WHERE MediaXrefs.TableID=108 AND  LOWER(FileName) LIKE '%.jpg' OR LOWER(FileName) LIKE '%.jpeg'),\
c as (\
SELECT  b.FileID, MediaMaster.Copyright,  ROW_NUMBER() over (partition by b.FileID order by MediaRenditions.RenditionID) as cpt_copy FROM b inner join \
MediaFiles on b.FileID=MediaFiles.FileID \
inner join   MediaRenditions ON MediaFiles.RenditionID = MediaRenditions.RenditionID \
INNER JOIN MediaMaster ON MediaRenditions.MediaMasterID = MediaMaster.MediaMasterID \
) \
select b.*, c.Copyright from b LEFT JOIN c ON b.fileID=c.FileID AND cpt_copy=1"
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    

def get_constituents_alt_names(p_pnd_cons):
    global global_db
    sql="SELECT AltNameID, ConstituentID as fk_constituent_id, DisplayName AS alternate_names FROM ConAltNames WHERE DisplayName IS NOT NULL"
    data=pnd.read_sql(sql=sql, con=global_db)    
    data=data.sort_values(by='AltNameID',ascending=True)
    unique_people=p_pnd_cons[['ConstituentID', 'DisplayName']].drop_duplicates(keep='first')
    data=pnd.merge(data, unique_people, how='left', left_on="fk_constituent_id", right_on="ConstituentID", suffixes=('_y', ''))
    data=data.drop(data.filter(regex='_y$').columns, axis=1)
    data=data.loc[data['DisplayName'].notna()]
    data['alternate_names']= data['alternate_names'].str.replace(r'\s+', ' ', regex=True)
    data=data.loc[data['alternate_names']!=data['DisplayName']]
    data=data[["fk_constituent_id", "DisplayName","alternate_names"]]
    data=data.groupby(["fk_constituent_id", "DisplayName"])["alternate_names"].apply(list).reset_index(name='v_cons_all_names')
    data["v_cons_all_names_implode"]=data.apply(lambda x: x['DisplayName'] + " : "+ "; ".join(x['v_cons_all_names']), axis=1)
    data=data[["fk_constituent_id","v_cons_all_names", "v_cons_all_names_implode"]]
    p_pnd_cons=pnd.merge(p_pnd_cons,data, how='left', left_on="ConstituentID", right_on="fk_constituent_id", suffixes=('_y', '') )
    return p_pnd_cons    
    
def get_constituents():
    global global_db
    sql="SELECT ConXrefDetails.ConstituentID ,ConXrefs.ID object_id,  ConXrefs.RoleID, DisplayName FROM ConXrefDetails inner join  ConXrefs \
on ConXrefs.TableID=108 AND ConXrefs.ConXrefID = ConXrefDetails.ConXrefID \
AND ConXrefDetails.UnMasked = 0 \
inner join constituents on  ConXrefDetails.ConstituentID=constituents.ConstituentID  "
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
def get_titles():
    global global_db
    sql="SELECT ObjTitles.Title,ObjTitles.ObjectID object_id, ObjTitles.TitleTypeID FROM ObjTitles"
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
def get_relationships():
    global global_db
    sql="with a as ( \
SELECT Associations.ID1 object_id, Associations.ID2 related_object, 'main' as rel_type,  \
Associations.RelationshipID, Relation1, Relation2 FROM TMS.dbo.Associations  \
  INNER JOIN TMS.dbo.[Relationships] ON Associations.RelationshipID=Relationships.RelationshipID WHERE Associations.TableID = 108 \
UNION   \
SELECT Associations.ID2, Associations.ID1, 'reversed' as rel_type, Associations.RelationshipID,  Relation1, Relation2 FROM   TMS.dbo.Associations INNER JOIN TMS.dbo.[Relationships] ON Associations.RelationshipID=Relationships.RelationshipID WHERE Associations.TableID = 108 ) ,  \
b as (select Objects.objectid, objectnumber FROM  \
TMS.dbo.Objects) , \
c as (  \
select distinct object_id, b.objectnumber as main_object,Objects.ObjectID related_object_id, Objects.objectnumber as related_object_number, Relation1, Relation2  from a   \
INNER JOIN b ON a.object_id=b.objectid  \
INNER JOIN  TMS.dbo.Objects ON a.related_object=Objects.objectid   \
where b.objectnumber != Objects.objectnumber  \
),  \
e as (SELECT object_id, main_object, related_object_number, Relation1 as relation_type_tms FROM c  \
UNION  \
select related_object_id, related_object_number, main_object, Relation1  from c )  \
select distinct * from e  "
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
def get_exhibitions():
    global global_db
    sql="with a as (SELECT  ExhObjXrefs.ObjectID object_id , \
COALESCE(ExhVenObjXrefs.BeginDisplDateISO, ExhVenuesXrefs.BeginISODate, '????') begin_date,  \
COALESCE(ExhVenObjXrefs.EndDisplDateISO, ExhVenuesXrefs.EndISODate, '????') end_date,  \
Exhibitions.ExhTitle, Constituents.DisplayName, ConAddress.City, Countries.Country \
FROM  \
[TMS].[dbo].[ExhVenuesXrefs] LEFT JOIN [TMS].[dbo].Constituents ON ExhVenuesXrefs.ConstituentID = Constituents.ConstituentID \
LEFT JOIN [TMS].[dbo].ConAddress ON Constituents.ConstituentID = ConAddress.ConstituentID \
  LEFT JOIN [TMS].[dbo].Countries ON ConAddress.CountryID = Countries.CountryID \
  LEFT JOIN [TMS].[dbo].ExhVenObjXrefs ON ExhVenuesXrefs.ExhVenueXrefID = ExhVenObjXrefs.ExhVenueXrefID \
  LEFT JOIN [TMS].[dbo].Exhibitions LEFT JOIN  [TMS].[dbo].ExhObjXrefs ON Exhibitions.ExhibitionID = ExhObjXrefs.ExhibitionID \
  ON ExhVenuesXrefs.ExhibitionID = Exhibitions.ExhibitionID AND ExhVenObjXrefs.ObjectID = ExhObjXrefs.ObjectID WHERE ConAddress.Rank=1  \
) \
select object_id, \
'['+begin_date+' - '+end_date+'] \"'+coalesce(ExhTitle,'')+'\" - '+COALESCE(DisplayName,'')+ ' ('+COALESCE(City,'')+ ' - '+COALESCE(Country,'')+')' as exhibition_text \
  from a"
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
def get_image_object(p_pnd_image, object_id):
    p_criteria=p_pnd_image[p_pnd_image["p_pnd_image"]==object_id]
    
    
def find_thesau_parent_term( p_pnd_thesau_hierarch, term_id, type_id=None, table_name=None):
    if type_id is not None:
        if table_name is not None:
            find_term=p_pnd_thesau_hierarch[(p_pnd_thesau_hierarch['base_term_id']==term_id) & (p_pnd_thesau_hierarch['type_id']==type_id) &  (p_pnd_thesau_hierarch['main_table'] ==table_name)]
        else:
            find_term=p_pnd_thesau_hierarch[(p_pnd_thesau_hierarch['base_term_id']==term_id) & (p_pnd_thesau_hierarch['type_id']==type_id)]
    else :
        find_term=p_pnd_thesau_hierarch[p_pnd_thesau_hierarch['base_term_id']==term_id]
    linked=find_term.sort_values(['base_term_id', 'CN'], ascending=[True, False])
    #print(linked)
    returned=[]
    for i, row in linked.iterrows():
        returned.append(row["Term"])
    return returned
   
def find_thesau_parent( p_pnd_thesau_hierarch, object_id , type_id):
    returned=[]
    filter_p=p_pnd_thesau_hierarch[(p_pnd_thesau_hierarch["type_id"]==type_id) & (p_pnd_thesau_hierarch["object_id"]==object_id)]
    if len(filter_p)>0:
        returned=filter_p["keyword_str"].unique().tolist()        
    return returned
    
def find_thesau_parent_syno( p_pnd_thesau_hierarch, object_id , type_id):
    returned1=[]
    returned2=[]
    returned3=[]
    filter_p=p_pnd_thesau_hierarch[(p_pnd_thesau_hierarch["type_id"]==type_id) & (p_pnd_thesau_hierarch["object_id"]==object_id)]
    if len(filter_p)>0:      
        returned1=filter_p["keyword_str"].unique().tolist()
        for i, row in filter_p.iterrows():
            returned2=returned2+row["list_syno"]
            if row["base_level"]>= THRESHOLD_GEO_KEYWORD:
                returned3=returned3+row["v_alt_terms"]
    returned2=list(set(returned2))
    return returned1, returned2, returned3


    
def find_thesau_flat(p_pnd_terms,  object_id, type_id ):    
    find_term=p_pnd_terms[(p_pnd_terms['object_id']==object_id) & (p_pnd_terms['type_id']==type_id)]
    return find_term["Term"].unique().tolist()
    

def unique_clean(p_frame, p_field_search, p_object_id, p_display_field=None):
    if p_display_field is  None:
        p_display_field=p_field_search
    returned=p_frame[p_frame[p_field_search]==p_object_id][p_display_field].dropna().unique()
    if returned is not None:
        returned=list(map(lambda x:' '.join(str(x).split()).strip(), returned))
    return returned 
    
#see classes   /var/www/non-public/gits/collections/routes/web/php and   /var/www/non-public/gits/collections/app/Helpers/BusinessLogic/TmsFunctions.php in Roel's Laravel on CG
def url_exists(url):
    response = requests.get(url)
    if response.status_code == 200:
        #print(response.headers)
        return True,response.headers["Content-Length"]
    else:
        return False, 0
        
def file_exists(path):
    response =os.path.isfile(path)
    if response:
        #print(response.headers)
        return True,os.path.getsize(path)
    else:
        return False, 0

def get_image_type_from_filename(filename):
    result="unknown"
    try:
        start_pos=filename.index("_")
        end_pos=filename.index("_", start_pos+1)
        result=filename[start_pos+1: end_pos].lower()
        if result=="fiche":
            result="eo-fiche"
            try:
                start_dot=filename.index(".")
                if start_dot==2:
                    if filename[1].lower()=="p":
                        return "photo-fiche"
            except ValueError as e:
                return result
        return result
    except Exception as e:
        return ""
    except ValueError  as e:
        return ""
        
def get_image_info_from_filename(p_file):
    p_basename=os.path.basename(p_file)

def parse_tms_image_path(givenImage, modus = "thumb" ):
    if modus=="large":
        paths=global_storage["imagePathsLarge"]
    elif modus=="original":
        paths=global_storage["imagePathsOriginal"] 
    else:
        paths=global_storage["imagePathsThumbs"]
    newurl=givenImage.replace('\\', '/')
    for key, value in paths.items():
        newurl=newurl.replace(key, value)
    if modus !="original":
        newurl=global_storage["gazelleImagesWebserver"]+newurl
    #newurl=newurl.replace("_roel","")
    return newurl
    
def parse_tms_image_path_nas(givenImage):   
    paths=global_storage["imagePathsThumbsNAS"]
    newurl=givenImage.replace('\\', '/')
    for key, value in paths.items():
        newurl=newurl.replace(key, value)
    #newurl=newurl.replace("/gazelle/", "/gazelle_roel/")
    return newurl


def resize_and_copy(object_id, img, img_path, prefix_original, ratio, prefix_thumb):
    global LOGGER
    global I_NEW_IMAGE
    img_thumb = cv2.resize(img, (0,0), fx=ratio, fy=ratio)
    #print("PHYSICAL 1 "+img_path)
    #print("replace "+prefix_original+ " by "+prefix_thumb)
    path_thumb=img_path.replace(prefix_original, prefix_thumb)
    #print("PHYSICAL 2 "+path_thumb)
    
    folder_thumb="/".join(path_thumb.split("/")[:-1])
    
    if not  os.path.exists(folder_thumb):
        try:
            Path(folder_thumb).mkdir(parents=True, exist_ok=True)
            LOGGER.info(folder_thumb + " created for object "+str(object_id))
        except Exception as e:
            LOGGER.info("Error "+path_thumb+" cannot be copied in  "+folder_thumb + " (cannot create  folder) for object "+str(object_id))
    if os.path.exists(folder_thumb):
        #print("folder exists")
        flag=cv2.imwrite(path_thumb, img_thumb)
        '''
        if flag:
            print(path_thumb+" created")
        else:
            print(path_thumb+" not created!!!")
        '''
        I_NEW_IMAGE=I_NEW_IMAGE+1
        returned=path_thumb
        LOGGER.info("image "+ path_thumb+" created for object "+str(object_id))
        return file_exists(path_thumb)
    else:
        LOGGER.info("Error "+path_thumb+" cannot be copied in  "+folder_thumb + " (missing folder) for object "+str(object_id))
    return False, 0
    
def prepare_copy(object_id, img,  prefix_original, prefix_check ):
    global MAX_SIZE_BIG
    global MAX_FILE_THUMB
    global IMG_TARGET
    global LOGGER
    global IMG_SRC
    #print(img)
    #print("replace "+prefix_original+ " by "+prefix_check)
    img_original=img.replace(prefix_original, prefix_check).replace("//","/")
    #print(img_original)
    #= test
    to_copy=False
    #print("TEST "+img_original)
    for test_img in IMG_SRC:
        #print(test_img)
        if img_original.startswith(test_img):
            #print("copy "+img_original)
            if file_exists(img_original):
                #print("EXISTS")
                img = cv2.imread(img_original)
                if img is not None:
                    height, width, channels = img.shape
                    #print(height)
                    #print(width)
                    max_size=max(height, width)
                    #print(max_size)
                    ratio_thumb=MAX_FILE_THUMB/max_size
                    #print(ratio_thumb)
                    ratio_big=MAX_SIZE_BIG/max_size
                    #print(ratio_big)
       
                    returned_tmp, size_tmp=resize_and_copy(object_id, img, img_original, prefix_check, ratio_big, IMG_TARGET["600px"])
                    if returned_tmp:
                        returned, size=resize_and_copy(object_id, img, img_original, prefix_check, ratio_thumb, IMG_TARGET["thumbs"])
                        return returned, size
                else:
                    LOGGER.info("Error "+img_original+" is None in NAS for object "+str(object_id))
            else:
                LOGGER.info("Error "+img_original+" declared in TMS but absent from NAS for object "+str(object_id))
            to_copy=True
    return False, 0
    
def parse_single_image(object_id, row):
    global global_check_exists
    global IMG_PREFIX_CHECK
    global IMG_PREFIX_ORIGINAL
    global COPY_IMG
    global IMG_MAPPING_SECURITY
    global url_iif
    global url_download
    result={}
    try:
        cpt_path=row["PhysicalPath"]+"\\"+row["FileName"]
        
        file=parse_tms_image_path_nas(cpt_path)#.split("//")
        
        file_security=file
        if len(file_security)>1:
            exists, filesize=file_exists(file_security)
            if not exists:
                #print(file_security+ " NOT FOUND")
                if COPY_IMG:
                    prepare_copy(object_id, file_security, IMG_PREFIX_ORIGINAL, IMG_PREFIX_CHECK)
                    exists, filesize=file_exists(file_security)
            #else:
            #    print(file_security+ " FOUND")
            if exists:
                #print("go")
                iiif=cpt_path.replace("\\", "/")
                large=iiif
                thumbs=iiif
                for key, item in global_storage["imagePathsOriginal"].items():
                    iiif=iiif.replace(key, item)
                phys_path=iiif
                iiif=url_iif+iiif
                for key, item in global_storage["imagePathsLarge"].items():
                    large=large.replace(key, item)
                large=global_storage["gazelleImagesWebserver"]+large
                for key, item in global_storage["imagePathsThumbs"].items():
                    thumbs=thumbs.replace(key, item)
                thumbs=global_storage["gazelleImagesWebserver"]+thumbs
                rend_id=str(row['RenditionID']) or None
                result={
                    'id_object': row["object_id"],
                    'id':row["RenditionID"],
                    'photoid':row["RenditionID"],
                    'type':get_image_type_from_filename(row["FileName"]  ),
                    'path':phys_path,
                    'filesize':filesize,
                    'zoomable_url':iiif,
                    '600px_url':large,
                    'download_url':url_download+rend_id,
                    'thumbnail_url':thumbs,
                    'copyright_text' :row['Copyright'] or " ",
                    'primary_image' :  row['PrimaryDisplay'],
                     'source' : 'tms'           
                }
                #print(result)
    except Exception as e:
        #print ("Error: unable to fetch data")
        #print(e)
        #traceback.print_exc()
        LOGGER.error("Error: unable to create image")
        LOGGER.error(e)
        LOGGER.error(traceback.print_exc())
    return result
  
def transform_nested_to_array_in_dict(items):
    global I_IMAGE
    returned={}
    for item in items:
        prefix="secondary_image_"
        if "primary_image" in item:
            if item["primary_image"]==1:
                prefix="primary_image_"
        for key, val in item.items():
            if key != 'id'  and key != 'primary_image':
                key=prefix+key
                if not key in returned:
                    returned[key]=[]
                returned[key].append(val)
        I_IMAGE=I_IMAGE+1
    return returned
    
  
def parse_images(object_id, pnd_images):
    metadata_images=[]
    for i, row in pnd_images.iterrows():
        metadata_images.append(parse_single_image(object_id, row))
    return metadata_images
    
   

def get_mapped_dict_syno(pnd_source, object_id, p_mapping_dict, type_key, returned_col, additional_columns, object_key="object_id"):
    returned={}
    returned2={}
    for col in additional_columns:
        returned2[col]=[]
    cons_filter=pnd_source[pnd_source[object_key]==object_id]
    roles=cons_filter[type_key].dropna().unique()
    for role in roles:
        if role in p_mapping_dict:
            cons_filter_2=cons_filter[cons_filter[type_key]==role]
            if len(cons_filter_2)>0:
                val=cons_filter_2[returned_col].dropna().unique()
                returned[p_mapping_dict[role]]=val
                for key, row in cons_filter_2.iterrows():
                    for col in additional_columns:
                        tmp=row[col]
                        if isinstance(tmp, list):
                            returned2[col]=returned2[col]+tmp
                        else:
                            returned2[col].append(tmp)
    for key, val in returned2.items():
        returned2[key]=list(set(val))
        returned2[key] = [x for x in returned2[key] if str(x) != 'nan']

    return returned, returned2
    
def get_mapped_dict(pnd_source, object_id, p_mapping_dict, type_key, returned_col, object_key="object_id"):
    returned={}
    cons_filter=pnd_source[pnd_source[object_key]==object_id]
    roles=cons_filter[type_key].dropna().unique()
    for role in roles:
        if role in p_mapping_dict:
            cons_filter_2=cons_filter[cons_filter[type_key]==role]
            val=cons_filter_2[returned_col].dropna().unique()
            returned[p_mapping_dict[role]]=val
        #else:
        #    print("cons key problem for "+str(role))
    return returned

def check_tms():
    global global_db
    sql="SELECT DISTINCT ObjectID object_id FROM Objects"
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
def delete_not_in_tms(solr):
    global LOGGER
    try:
        #print("DELETE NOT in tms")
        solr = pysolr.Solr(SOLR_ACCESS_POINT)
        result=solr.search("*:*", start=0, rows=0)
        cpt=result.raw_response['response']['numFound']
        #print(cpt)
        pages=int(math.ceil(cpt/SIZE))
        #print(pages)
        pnd_tms=check_tms()
        #print(pnd_tms)
        columns = ['id', 'collection_number', 'short_description']

        df = pnd.DataFrame(columns=columns)
        for i in range(0,pages):
            current=i*SIZE
            result=solr.search("*:*", start=current, rows=SIZE, sort="sortnumber asc")
            for rec in result:
                t_id=rec["id"]
                number=rec["objectnumber"]
                if "brief_description" in rec:
                    desc=rec["brief_description"]
                else:
                    desc=None
                #print(t_id)
                '''
                print(number)
                print(desc)
                '''
                pnd_filter=pnd_tms[pnd_tms["object_id"]==int(t_id)]
                
                if len(pnd_filter)==0:
                    #print(t_id)
                    #print("NOT_FOUND")
                    df.loc[len(df)]={"id":t_id, "collection_number":number, "short_description":desc }
                    solr.delete(q="id:"+str(t_id))
                    solr.commit()
            #print(current)
            #print(print_time())
        for i, row in df.iterrows():
            #print(row) 
            LOGGER.info("DELETED\t"+str(row["id"])+"\t"+str(row["collection_number"]))             
    except Exception as e:
      #print ("Error: unable to fetch data")
      #print(e)
      #traceback.print_exc()
      LOGGER.error("Error: unable to fetch data_DELETE")
      LOGGER.error(e)
      LOGGER.error(traceback.print_exc())

def clean_geo_names(p_str):
    p_str=[  re.sub('(world\s+-)|(continents\s+-)|(africa\s+-)|(nations\s+-)|(rivers\s+-)|(lakes\s+-)|(cultures\s+-)|(regions\s+\(general\)\s+-)|(regions \(historical cross-border\)\s=-)','', x, flags=re.IGNORECASE).strip().strip("-") for  x in  p_str]
    return p_str

  
def generate_solr_json(row_main, pnd_terms, merge_hierarch, pnd_relationships, pnd_images, pnd_constituents, pnd_titles, pnd_exhibitions, pnd_alt_nums):

    object_id=row_main['ObjectID']
    #print("OBJECT")
    #print(object_id)
    typology=find_thesau_flat(pnd_terms,  object_id, 41)
    typology_old=find_thesau_flat(pnd_terms, object_id, 31)
    typology=typology+typology_old
    material=find_thesau_flat(pnd_terms,  object_id, 28)
    cultures=find_thesau_flat(pnd_terms,  object_id, 44)
    cultures_production=find_thesau_flat(pnd_terms, object_id, 49)
    
    list_alt_geo_names=[]
    list_alt_geo_names_base=[]
    geo_represented_tmp, list_alt_geo_temp, base_syno_tmp=find_thesau_parent_syno( merge_hierarch, object_id,38)
    geo_represented=clean_geo_names(geo_represented_tmp)
    list_alt_geo_names=list_alt_geo_names+list_alt_geo_temp
    list_alt_geo_names_base=list_alt_geo_names_base+base_syno_tmp
    #geo_represented=clean_geo_names(find_thesau_parent( merge_hierarch, object_id,38))
    
    geo_depicted_tmp, list_alt_geo_temp, base_syno_tmp=find_thesau_parent_syno( merge_hierarch, object_id,51)
    geo_depicted=clean_geo_names(geo_depicted_tmp)
    list_alt_geo_names=list_alt_geo_names+list_alt_geo_temp
    list_alt_geo_names_base=list_alt_geo_names_base+base_syno_tmp
    #geo_depicted=clean_geo_names(find_thesau_parent( merge_hierarch, object_id,51))
    #print(geo_depicted)
    geo_collection_tmp, list_alt_geo_temp, base_syno_tmp=find_thesau_parent_syno( merge_hierarch, object_id,45)
    geo_collection=clean_geo_names(geo_collection_tmp)
    list_alt_geo_names=list_alt_geo_names+list_alt_geo_temp
    list_alt_geo_names_base=list_alt_geo_names_base+base_syno_tmp
    
    list_alt_geo_names=list(set(list_alt_geo_names))
    list_alt_geo_names_base=list(set(list_alt_geo_names_base))
    #print("GEO_ALT")
    #print(list_alt_geo_names)
    geo_all=geo_depicted+geo_collection
    #print(geo_all)
    countries=[x.split(" - ")[0].strip() for x in geo_all if len(x.split("-")) >0 ]
    #print(countries)
    images=pnd_images[pnd_images["object_id"]==object_id]
    #print("IMAGES")
    #print(images)
    image_metadata=parse_images(object_id, images)
    #print("IMAGES_METADATA")
    #print(image_metadata)
    
    images_for_solr=transform_nested_to_array_in_dict(image_metadata)
    #print("IMAGES_SOLR")
    #print(images_for_solr)
    
    
        
        
    doc={}
    doc["id"]=row_main["ObjectID"]
    doc["objectnumber"]=row_main["ObjectNumber"]
    doc["sortnumber"]=row_main["SortNumber"]
    doc["department"]=row_main["Department"]
    doc["brief_description"]=row_main["brief_description"]
    doc["acquisition_method"]=row_main["AccessionMethod"]
    doc["date_of_production"]=row_main["date_of_production"] 
    doc["date_of_production_str"]=row_main["date_of_production_str"]
    doc["production_year"]=row_main["production_year"]
    doc["date_of_collecting"]=row_main["date_of_collection"]  
    doc["date_of_acquisition"]=row_main["AccessionISODate"]
    doc["acquisition_year"]=row_main["acquisition_year"]
    doc["date_of_previous_acquisition"]=row_main["date_of_previous_acquisition_final_concat"]
    doc["dimensions"]=row_main["Dimensions"]
    doc["title"]=row_main["Title"]
    doc["exhibition_history"]=row_main["Exhibitions"]      
    doc["publications"]=row_main["PubReferences"]
    doc["description"]=row_main["Description"]
    doc["iiif_manifest"]=row_main["iiif_manifest"]
    doc["dated"]=row_main["Dated"]  
    doc["dateremarks"]=row_main["DateRemarks"]
    
    doc["culture"]=cultures
    doc["culture_of_production"]= cultures_production
    doc["geo_field_collection"]=geo_collection
    doc["geo_depicted"]=geo_depicted
    doc["geo_represented"]=geo_represented
    if len(countries)>0:
        doc["geo_country"]=list(set(countries))
    doc["v_geo_alt_names"]=list_alt_geo_names
    if len(list_alt_geo_names_base)>0:
        doc["v_discovery_geo_alt_names"]=list_alt_geo_names_base
    pnd_relationships_loc=pnd_relationships[pnd_relationships["object_id"]==object_id]
    if len(pnd_relationships_loc)>0:
        doc["see_also"]=pnd_relationships_loc["related_object_number"].tolist()
        doc["see_also_type"]=pnd_relationships_loc["relation_type_tms"].tolist()
    
    doc["typology"]=typology
    doc["materials"]=material
    
    const_array,const_array2 =get_mapped_dict_syno(pnd_constituents, object_id, mapping_cons, "RoleID", "DisplayName", ['v_cons_all_names','v_cons_all_names_implode'], )
    
    for key, val in const_array.items():
        doc[key]=val.tolist()
    if len(const_array2["v_cons_all_names"])>0:
        doc["v_cons_all_names"]=const_array2["v_cons_all_names"]
    if len(const_array2["v_cons_all_names_implode"])>0:
        doc["v_cons_all_names_implode"]=const_array2["v_cons_all_names_implode"]
    title_array=get_mapped_dict(pnd_titles, object_id, mapping_titles, "TitleTypeID", "Title")
    for key, val in title_array.items():
        tmp=val.tolist()
        doc[key]=tmp
        
    exhibition_array=pnd_exhibitions[pnd_exhibitions["object_id"]==object_id]
    doc["exhibition"]=exhibition_array["exhibition_text"].unique().tolist()
    doc["exhibition_facet"]=doc["exhibition"]
    
    alt_nums=unique_clean(pnd_alt_nums, "object_id",object_id, "alt_num_str")
    doc["alt_num_full"]=alt_nums
    doc["last_update_date"]=datetime.datetime.now().isoformat()
    if len(images_for_solr)>0:
        doc= {**doc, **images_for_solr}
    
    #print(row_main)

    return doc

def insert_solr(solr_end_point,row_main, pnd_terms, merge_hierarch, pnd_relationships, pnd_images, pnd_constituents, pnd_titles, pnd_exhibitions, pnd_alt_nums):
    global LOGGER
    try:
        doc=generate_solr_json(row_main, pnd_terms, merge_hierarch, pnd_relationships, pnd_images, pnd_constituents, pnd_titles, pnd_exhibitions, pnd_alt_nums)
        solr_end_point.add(doc)
    except Exception as e:
        LOGGER.error("ERROR IN INSERT_SOLR")
        LOGGER.error(e)
        LOGGER.error(traceback.print_exc())
    

def pattern_date(p_date):
    global pattern, pattern2

    tmp=pattern.match(p_date)
    if tmp is not None:
        returned= tmp[5]+'-'+tmp[3].rjust(2,'0')+'-'+tmp[1].rjust(2,'0')
        return returned
    else: 
        tmp2=pattern2.match(p_date)
        if tmp2 is not None:
            returned= tmp2[2].strip()+" "+tmp2[1].strip()+" "+tmp2[3].strip()
            return returned
        else:
            return p_date

def pattern_year(p_date):
    global pattern2, pattern3
    tmp1=pattern3.match(p_date)
    if tmp1 is not None:
        #return tmp1[2].strip()+'-'+ tmp1[4].strip()
        return tmp1[2].strip() + ' (approx.)' #+'-'+ tmp1[4].strip()
    else:
        tmp2=pattern2.match(p_date)
        if tmp2 is not None:
            returned= tmp2[2].strip()
            return returned
        else:
            return None
            
def go():
    global SOLR_ACCESS_POINT
    global global_db
    global I_IMAGE
    global I_NEW_IMAGE
    global LOG_FILE
    global LOGGER
    try:
        
        today = datetime.date.today()
        logging.basicConfig(filename=LOG_FILE.replace(".txt", "_"+today.strftime("%Y_%m_%d")+".txt"), level=logging.INFO,  format='%(asctime)s %(levelname)s %(name)s %(message)s', filemode='w')
        logging.getLogger("pysolr").setLevel(logging.WARNING)
        LOGGER=logging.getLogger(__name__)
        
        #all_objectids_list = []
        print_time()
        global_db=getDBConnection()
        solr = pysolr.Solr(SOLR_ACCESS_POINT)
        
        getAllObjectIdsToDo()
        pnd_main=getAllObjectIdsToDo()
        #print(pnd_main)
        pnd_description=get_obj_description()
        #print(pnd_description)
        pnd_main=pnd.merge(pnd_main, pnd_description, left_on='ctrl_id', right_on='ObjectID', how='inner')
        #print(pnd_main)
        pnd_terms=get_terms()
        list_synos=get_terms_alt_names()
        #print("TERMS")
        #print(pnd_terms)
        pnd_thesau_hierarch=get_thesaurus_hierarchy_for_syno()

        pnd_terms,pnd_thesau_hierarch= merge_terms_alt_names(list_synos,pnd_terms, pnd_thesau_hierarch )
        del list_synos
        gc.collect()
        #print("hierarch_syno")
        #print("------------------")
        #print(list(pnd_thesau_hierarch.columns))
        #print(pnd_thesau_hierarch)
        
        pnd_thesau_hierarch=create_key_word_string_syno(pnd_thesau_hierarch)
        #print("AFTER_AGG")
        #print(pnd_thesau_hierarch)
        merge_hierarch=pnd.merge(pnd_terms, pnd_thesau_hierarch, left_on='TermID', right_on='base_term_id', how='left')
        #print(pnd_thesau_hierarch)
        #print(merge_hierarch)
        del pnd_thesau_hierarch
        gc.collect()
        #print("MERGE_HIERARCH")
        #print(merge_hierarch)
        #merge_hierarch=merge_hierarch.sort_values(['object_id','base_term_id', 'CN'], ascending=[True, True, True])
        
        pnd_relationships=get_relationships()
        pnd_relationships=pnd_relationships.sort_values(['object_id','related_object_number'], ascending=[True, True])
        
        
        pnd_images=get_images()
        #print(pnd_images)
        
        pnd_constituents=get_constituents()
        pnd_constituents=get_constituents_alt_names(pnd_constituents)
        
        pnd_titles=get_titles()
        
        pnd_relationships=get_relationships()
        pnd_exhibitions=get_exhibitions()
        
        pnd_alt_nums=get_alt_nums()
        
        pnd_main["date_of_production_str"]=pnd_main["date_of_production"]
        pnd_main['date_of_production_str']=pnd_main.apply(lambda x:  pattern_date(x['date_of_production_str']), axis=1 )
        
        pnd_main["acquisition_year"]=None
        pnd_main['acquisition_year']=pnd_main.apply(lambda x:  pattern_year(x['AccessionISODate']), axis=1 )
        
        pnd_main["production_year"]=None
        pnd_main['production_year']=pnd_main.apply(lambda x:  pattern_year(x['date_of_production']), axis=1 )
        
        print("cpt objects "+str(len(pnd_main)))
        print("process")
        print_time()
        
        for i, row in pnd_main.iterrows():
            insert_solr(solr, row, pnd_terms, merge_hierarch, pnd_relationships, pnd_images, pnd_constituents, pnd_titles, pnd_exhibitions, pnd_alt_nums)
            if i % 1000 ==0:
                solr.commit()
                LOGGER.info("Imported "+str(i+1)+ " "+str(I_IMAGE+1)+ " images"+ ", "+str(I_NEW_IMAGE)+ " moved to Gazelle")
                print("Imported "+str(i+1)+ " "+str(I_IMAGE+1)+ " images"+ ", "+str(I_NEW_IMAGE)+ " moved to Gazelle")
                print_time()
        solr.commit()
       
        LOGGER.info("Imported "+str(i+1)+ " "+str(I_IMAGE+1)+ " images"+ ", "+str(I_NEW_IMAGE+1)+ " moved to Gazelle")
        LOGGER.info("load done")
        #print_time()
        
        delete_not_in_tms(solr)
        LOGGER.info("delete done")
        
    except Exception as e:
        #print ("Error: unable to fetch data")
        #print(e)
        #traceback.print_exc()
        LOGGER.error("ERROR IN MAIN")
        LOGGER.error(e)
        LOGGER.error(traceback.print_exc())
    finally:
        logging.shutdown()
        
if __name__ == "__main__":
    go()