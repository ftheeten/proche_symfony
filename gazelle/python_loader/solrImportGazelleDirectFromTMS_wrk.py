import pyodbc
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

#all_objectids_list = []
I_IMAGE=0
global_db=None
global_parents={}
SIZE=1000
LOG_FILE='/xx/xx/xx/xx/log_transfer_gazelle.txt'
LOGGER=None

SOLR_ACCESS_POINT="http://xxx/solr/gazelle"
global_check_exists='/xxx/'nas
global_storage_backup={
# images 'new' system
                'newImageHandlingImageServer' : "https://nas.be/",
                'newImageHandlingImagePrefix' : "https://nas.be/path/web/collections/okapi/",


                # images 'old' system

                'primaryImagePrefix': "https://nas.be/path/web/collections/eo-fiches/600px/",
                'thumbnailprefix' : "https://nas.be/path/web/collections/eo-fiches/thumbs/",
                'mountdir' : "/folder/path/collections_bck/path_1/collections/eo-fiches/fullsize/",


                'gazelleImagesWebserver' : "https://nas.be/path/web/collections/",

                'imagePathsThumbs' : {
                    '//MEDIAS/collections/TMS/' : "gazelle/thumbs/MEDIAS/collections/TMS/",
                    '//172.16.4.250/Qweb/' : "gazelle/thumbs/172.16.4.250/Qweb/",
                    '//path/tms/' : "gazelle/thumbs/path/tms/",
                    '//path/tms/' : "gazelle/thumbs/path/tms/",
                },
                'imagePathsLarge' : {
                    '//Mpath/collections/TMS/' : "gazelle/600px/Mpath/collections/TMS/",
                    '//SERVER/Qweb/' : "gazelle/600px/SERVER/Qweb/",
                    '//path/tms/' : "gazelle/600px/path/tms/",
                    '//path/tms/' : "gazelle/600px/path/tms/",
                 },
                'imagePathsOriginal' :{
                    '//Mpath/collections/TMS/' : "/folder/Mpath/collections/TMS/",
                    '//SERVER/Qweb/' : "/folder/SERVER/Qweb/",
                    '//path/tms/' : "/folder/path/tms-bck/tms-share/tms/",
                    '//path/tms/' : "/folder/path/tms-bck/path_tms/tms/",
                },
                'gazelle_tifs_prefix' : '/folder/path/tms-bck/path_tms-hq/tms-hq/',

                'gazelle_tifs_orig_prefix' : '/folder/path/tms-bck/path_tms-hq/tms-hq/',
}

global_storage={

                # okapi

                'okapi_image_server' : "https://nas.be/",
                'okapi_image_server_prefix' : "https://nas.be/collections/okapi/",
                'okapi_fullsize_basedir' : "/folder/path/collections/okapi/fullsize/",

               # images 'old' system -- eo-fiches

                'primaryImagePrefix' : "https://nas.be/collections/eo-fiches/600px/",
                'thumbnailprefix' : "https://nas.be/collections/eo-fiches/thumbs/",
                'mountdir' : "/folder/path/collections/eo-fiches/fullsize/",
                'eofiches_primary_image_prefix' : "https://nas.be/collections/eo-fiches/600px/",
                'eofiches_thumbnail_prefix' : "https://nas.be/collections/eo-fiches/thumbs/",
                'eofiches_fullsize_basedir' : "/folder/path/collections/eo-fiches/fullsize/",



                'gazelleImagesWebserver' : "https://nas.be/collections/",
                'imagePathsThumbsNAS' : {
                    
                    '//path/tms/' : "/folder/path/collections/gazelle/thumbs/path/tms/",
                },
                'imagePathsThumbs' : {
                    '//Mpath/collections/TMS/' : "gazelle/thumbs/Mpath/collections/TMS/",
                    '//SERVER/Qweb/' : "gazelle/thumbs/SERVER/Qweb/",
                    '//path/tms/' : "gazelle/thumbs/path/tms/",
                    '//path/tms/' : "gazelle/thumbs/path/tms/",
                },
                'imagePathsLarge' : {
                    '//Mpath/collections/TMS/' : "gazelle/600px/Mpath/collections/TMS/",
                    '//SERVER/Qweb/' : "gazelle/600px/SERVER/Qweb/",
                    '//path/tms/' : "gazelle/600px/path/tms/",
                    '//path/tms/' : "gazelle/600px/path/tms/",
                 },
                'imagePathsOriginal' : {
                    '//Mpath/collections/TMS/' : "/folder/Mpath/collections/TMS/",
                    '//SERVER/Qweb/' : "/folder/SERVER/Qweb/",
                    '//path/tms/' : "/folder/path/tms/",
                    '//path/tms/' : "/folder/path/tms/",
                 },
                'gazelle_tifs_prefix' : '/folder/path/tms-hq/',
                'gazelle_tifs_orig_prefix' : '/folder/path/tms-hq/',
           }

url_iif="https://ca.museum.africamuseum.be/collections/index.php/sd?file="

global_where="WHERE ObjectID > 0 "


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
1:"cons_schenker",
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

def print_time():
    now = datetime.datetime.now()
    print ("Current date and time : ")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))

def getDBConnection():
    #print("connect TMS")
    cnxn_str = ("Driver={ODBC Driver 18 for SQL Server};"
            "Server=db,;"
            "Database=??;"
            "UID=??;"
            "PWD=??$;"
            "TrustServerCertificate=yes;")
    db = pyodbc.connect(cnxn_str)
    return db

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
    data=data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    #print(data)
    #print(data.columns.tolist())
    data = data.replace({np.nan: ''})
    data["date_of_previous_acquisition_final_concat"]=""
    tmp = data.groupby(['ObjectID'], as_index=False)[['date_of_previous_acquisition_final']].agg(lambda x: ', '.join(map(str, set(x))))
  
    
    tmp = data.groupby(['ObjectID'], as_index=False)[['date_of_previous_acquisition_final']].agg(lambda x: ' - '.join(map(str, set(x))))
    #print(tmp)
    #print("-------------")
    tmp['date_of_previous_acquisition_final'] = tmp['date_of_previous_acquisition_final'].astype('str')
    tmp=tmp.applymap(lambda x: x.strip() if isinstance(x, str) else x)
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
    
    return returned

def get_terms():
    global global_db
    global global_where
    sql="with a0 as\
    (SELECT  DISTINCT ObjectID as ctrl_id FROM Objects "+ global_where +"),\
    a as\
(\
SELECT distinct ThesXrefs.TermID, ThesXrefs.ThesXrefTypeID as type_id,ThesXrefs.ID as object_id, 'ThesXrefs'  as main_table FROM  [TMS].[dbo].ThesXrefs WHERE ThesXrefs.TableID=108 \
UNION \
SELECT distinct ColThesXrefs.TermID, ColThesXrefs.ColThesXrefTypeID, ColThesXrefs.ID, 'ColThesXrefs'  FROM  [TMS].[dbo].ColThesXrefs WHERE ColThesXrefs.TableID=108 \
) \
select DISTINCT a.*, TermMasterID, Term from a INNER JOIN a0 ON a.object_id=a0.ctrl_id INNER JOIN [TMSThesaurus].[dbo].[Terms] ON a.TermID=[Terms].TermID"
    data=pnd.read_sql(sql=sql, con=global_db)
    return data
    
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
SELECT MediaFiles.FileName,\
REPLACe(MediaPaths.PhysicalPath, '\\mpath\collections\TMS\','\\Mpath\collections\TMS\' ) \
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
    
def get_constituents():
    global global_db
    sql="SELECT ConXrefDetails.ConstituentID ,ConXrefs.ID object_id,  ConXrefs.RoleID, DisplayName FROM ConXrefDetails inner join  ConXrefs \
on ConXrefs.TableID=108 AND ConXrefs.ConXrefID = ConXrefDetails.ConXrefID \
AND ConXrefDetails.UnMasked = 0 \
inner join constituents on  ConXrefDetails.ConstituentID=constituents.ConstituentID"
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
    


    
def find_thesau_flat(p_pnd_terms,  object_id, type_id, table_name="None" ):
    
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
    return newurl
    
def parse_tms_image_path_nas(givenImage):   
    paths=global_storage["imagePathsThumbsNAS"]
    newurl=givenImage.replace('\\', '/')
    for key, value in paths.items():
        newurl=newurl.replace(key, value)    
    return newurl
    
def parse_single_image(row):
    global global_check_exists
    result={}
    cpt_path=row["PhysicalPath"]+"\\"+row["FileName"]        
    #file_tmp=parse_tms_image_path(cpt_path).split("//")
    file=parse_tms_image_path_nas(cpt_path)#.split("//")
    
    if len(file)>1:
        exists, filesize=file_exists(file)
        if exists:
            iiif=cpt_path.replace("\\", "/")
            large=iiif
            thumbs=iiif
            for key, item in global_storage["imagePathsOriginal"].items():
                iiif=iiif.replace(key, item)
            iiif=url_iif+iiif
            for key, item in global_storage["imagePathsLarge"].items():
                large=large.replace(key, item)
            large=global_storage["gazelleImagesWebserver"]+large
            for key, item in global_storage["imagePathsThumbs"].items():
                thumbs=thumbs.replace(key, item)
            thumbs=global_storage["gazelleImagesWebserver"]+thumbs
            result={
                'id':row["object_id"],
                'photoid':row["object_id"],
                'type':get_image_type_from_filename(row["FileName"]  ),
                'path':file,
                'filesize':filesize,
                'zoomable_url':iiif,
                '600px_url':large,
                'thumbnail_url':thumbs,
                'copyright_text' :row['Copyright'],
                'primary_image' :  row['PrimaryDisplay'],
                 'source' : 'tms'           
            }
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
            if key != 'id' and key != 'photoid' and key != 'primary_image':
                key=prefix+key
                if not key in returned:
                    returned[key]=[]
                returned[key].append(val)
        I_IMAGE=I_IMAGE+1
    return returned
    
  
def parse_images(pnd_images):
    metadata_images=[]
    for i, row in pnd_images.iterrows():
        metadata_images.append(parse_single_image(row))
    return metadata_images
    
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
                    solr.delete("id"+str(t_id))
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

      
def generate_solr_json(row_main, pnd_terms, merge_hierarch, pnd_relationships, pnd_images, pnd_constituents, pnd_titles, pnd_exhibitions, pnd_alt_nums):

    object_id=row_main['ObjectID']
    #print("OBJECT")
    #print(object_id)
    typology=find_thesau_flat(pnd_terms,  object_id, 41, "ThesXrefs")
    typology_old=find_thesau_flat(pnd_terms, object_id, 31, "ThesXrefs")
    typology=typology+typology_old
    material=find_thesau_flat(pnd_terms,  object_id, 28, "ThesXrefs")
    cultures=find_thesau_flat(pnd_terms,  object_id, 44, "ThesXrefs")
    cultures_production=find_thesau_flat(pnd_terms, object_id, 49, "ThesXrefs")
    
    geo_represented=find_thesau_parent( merge_hierarch, object_id,38)
    geo_depicted=find_thesau_parent( merge_hierarch, object_id,51)
    #print(geo_depicted)
    geo_collection=find_thesau_parent( merge_hierarch, object_id,45)
    
    images=pnd_images[pnd_images["object_id"]==object_id]
    #print("IMAGES")
    #print(images)
    image_metadata=parse_images(images)
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
    doc["date_of_collecting"]=row_main["date_of_collection"]  
    doc["date_of_acquisition"]=row_main["AccessionISODate"]
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
    
    pnd_relationships_loc=pnd_relationships[pnd_relationships["object_id"]==object_id]
    if len(pnd_relationships_loc)>0:
        doc["see_also"]=pnd_relationships_loc["related_object_number"].tolist()
        doc["see_also_type"]=pnd_relationships_loc["relation_type_tms"].tolist()
    
    doc["typology"]=typology
    doc["materials"]=material
    
    const_array=get_mapped_dict(pnd_constituents, object_id, mapping_cons, "RoleID", "DisplayName")
    for key, val in const_array.items():
        doc[key]=val.tolist()
    
    title_array=get_mapped_dict(pnd_titles, object_id, mapping_titles, "TitleTypeID", "Title")
    for key, val in title_array.items():
        doc[key]=val.tolist()
        
    exhibition_array=pnd_exhibitions[pnd_exhibitions["object_id"]==object_id]
    doc["exhibition"]=exhibition_array["exhibition_text"].unique().tolist()
    doc["exhibition_facet"]=doc["exhibition"]
    
    alt_nums=unique_clean(pnd_alt_nums, "object_id",object_id, "alt_num_str")
    doc["alt_num_full"]=alt_nums
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
      
def go():
    global SOLR_ACCESS_POINT
    global global_db
    global I_IMAGE
    global LOG_FILE
    global LOGGER
    try:
        logging.basicConfig(filename=LOG_FILE, level=logging.INFO,  format='%(asctime)s %(levelname)s %(name)s %(message)s', filemode='w')
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
        #print("TERMS")
        #print(pnd_terms)
        pnd_thesau_hierarch=get_thesaurus_hierarchy()
        merge_hierarch=pnd.merge(pnd_terms, pnd_thesau_hierarch, left_on='TermID', right_on='base_term_id', how='left')
        #print("MERGE_HIERARCH")
        #print(merge_hierarch)
        #merge_hierarch=merge_hierarch.sort_values(['object_id','base_term_id', 'CN'], ascending=[True, True, True])
        
        pnd_relationships=get_relationships()
        pnd_relationships=pnd_relationships.sort_values(['object_id','related_object_number'], ascending=[True, True])
        
        
        pnd_images=get_images()
        #print(pnd_images)
        pnd_constituents=get_constituents()
        pnd_titles=get_titles()
        
        pnd_relationships=get_relationships()
        pnd_exhibitions=get_exhibitions()
        
        pnd_alt_nums=get_alt_nums()
        

        for i, row in pnd_main.iterrows():
            insert_solr(solr, row, pnd_terms, merge_hierarch, pnd_relationships, pnd_images, pnd_constituents, pnd_titles, pnd_exhibitions, pnd_alt_nums)
            if i % 1000 ==0:
                solr.commit()
                LOGGER.info("Imported "+str(i+1)+ " "+str(I_IMAGE+1)+ " images")
                #print_time()
        solr.commit()
       
        LOGGER.info("Imported "+str(i+1)+ " "+str(I_IMAGE+1)+ " images")
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