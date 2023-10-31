import pyodbc 
import pysolr
import datetime
import traceback
import sys
import pandas as pnd
from collections import OrderedDict
import sqlalchemy as sa

print("init")

global_terms={}
solr_url='http://193.190.223.32:8983/solr/proche-symfony/'

def test_connection(cn, constr):
    rebuild=False
    if cn is None:
        rebuild=True
    elif not cn:
        rebuild=True
    if not rebuild:
        cn= sa.create_engine(constr)
    return cn
    
def handle_images(cursor, doc, objid):
    doc["thumbnail_path_ss"]="https://iiif.africamuseum.be/fotos%2Fdigit-qa%2FAA.0-A.1%2F0003.jp2/full/!150,150/0/default.jpg"
    doc["image_path_ss"]="https://iiif.africamuseum.be/fotos%2Fdigit-qa%2FAA.0-A.1%2F0003.jp2/full/full/0/default.jpg"
    doc["iiif_manifest_ss"]="https://manifests.africamuseum.be/smartwood/Smartwood_first_edition.json"
    doc["iiif_image_endpoint_ss"]="https://iiif.africamuseum.be/fotos%2Fdigit-qa%2FAA.0-A.1%2F0003.jp2/info.json"


def get_titles(cursor, doc, objid, title_type, fieldname ):
    sql="SELECT DISTINCT ObjTitles.Title FROM ObjTitles WHERE ObjTitles.ObjectID=? AND ObjTitles.TitleTypeID=?"
    rs=cursor.execute(sql,[objid, title_type ])
    rows = rs.fetchall()
    returned=[]
    for row in rows:
        if row is not None:
            if len(row[0].strip())>0:
                returned.append(row[0].strip())
    if len(returned)>0:
        doc[fieldname]=returned
        
def get_images(doc, pimages,  pfilter, fieldname):
    images=[]
    pimagesfilter=pimages.loc[pfilter]
    if len(pimagesfilter)>0:
        for idx, row in pimagesfilter.iterrows():
            images.append(row["FileName"])
    images=list(set(images))
    if len(images)>0:
        doc[fieldname]=images

def get_thesaurus_term(doc, pxref, pterms, pmaster, pfilter, fieldname):
    sum_terms=[]
    pxref_filter=pxref.loc[pfilter]
    if len(pxref_filter)>0:
        #print("======================>"+fieldname+" FOUND")
        for rowfilter in pxref_filter:
            p_terms_filter=pterms.merge(pxref_filter, on='TermID')
            if len(p_terms_filter)>0:
                p_terms_filter=p_terms_filter.loc[:, [ 'Term']]
                p_terms_filter["Term"].drop_duplicates()
                for idx, rowterm in p_terms_filter.iterrows():
                    sum_terms.append(rowterm["Term"])
    if len(sum_terms)>0:
        sum_terms=list(OrderedDict.fromkeys(sum_terms))
        #print(sum_terms)
        doc[fieldname]=sum_terms
                    
                    
def get_thesaurus_term_hierarchy(doc, pxref, pterms, pmaster, pterms2, pmaster2, pfilter, fieldname):
    global global_terms
    sum_terms=[]
    pxref_filter=pxref.loc[pfilter]
    if len(pxref_filter)>0:
        #print("======================>"+fieldname+" FOUND")
        for rowfilter in pxref_filter:
            #print(pxref_filter["TermID"])            
            #p_terms_filter=pterms.loc[pterms["TermID"]==pxref_filter["TermID"]]
            p_terms_filter=pterms.merge(pxref_filter, on='TermID')
            
            if len(p_terms_filter)>0:
                p_terms_filter=p_terms_filter.merge(pmaster, on='TermMasterID', how='left')
                p_terms_filter=p_terms_filter.loc[:, ['TermID', 'Term', 'CN']]
                p_terms_filter["Term"].drop_duplicates()
                for idx, rowterm in p_terms_filter.iterrows():
                    terms={}
                    if rowterm["TermID"] in global_terms:
                        terms=global_terms[rowterm["TermID"]]
                    else:
                        if not rowterm["Term"] in terms:
                            terms[rowterm["Term"]]=rowterm["CN"]
                            if rowterm["CN"] is not None:
                                if len(rowterm["CN"].strip())>0:
                                    path=rowterm["CN"].split(".")
                                    while len(path)>0:
                                        path.pop()
                                        new_path='.'.join(path)
                                        pmaster_parent=pmaster2.loc[pmaster["CN"]==new_path]
                                        if len(pmaster_parent)>0:
                                            pmaster_parent=pmaster_parent.merge(pterms2, on='TermMasterID')
                                            if len(pmaster_parent)>0:
                                                pmaster_parent=pmaster_parent.loc[:,['Term', 'CN']]
                                                pmaster_parent.drop_duplicates()
                                                for idx2, rowparent in pmaster_parent.iterrows():
                                                    if not rowparent["Term"].isnumeric():
                                                        terms[rowparent["Term"]]=rowparent["CN"]
                        global_terms[rowterm["TermID"]]=terms
                        sum_terms=sum_terms+list(terms.keys())
    if len(sum_terms)>0:
        sum_terms=list(OrderedDict.fromkeys(sum_terms))
        #print(sum_terms)
        doc[fieldname]=sum_terms
                       

def get_site_of_collection(doc, cursor, objid):
    returned=[]
    sql="SELECT longtext7 FROM ObjContext WHERE objectid = ?"
    rs=cursor.execute(sql, [objid])
    rows = rs.fetchall() 
    for row in rows:
        if row[0] is not None:
            returned.append(row[0])
    if len(returned)>0:
        #print("===>geo_field_collection")
        #print(returned)
        doc["geo_field_collection"]=returned
        
def get_images_panda(conn):
    sql="SELECT DISTINCT ObjectId, FileName, ObjectNumber, PrimaryDisplay from MediaFiles\
        INNER JOIN MediaMaster\
        ON MediaFiles.RenditionId=MediaMaster.DisplayRendID\
        INNER JOIN Mediarenditions\
        ON MediaMaster.DisplayRendID=Mediarenditions.RenditionID\
        INNER JOIN [Objects]\
        ON Mediarenditions.sortnumber=objects.SortNumber\
        INNER JOIN\
        MediaXrefs\
        ON Objects.objectid=MediaXrefs.ID and TableID=108"
    data=pnd.read_sql(sql=sql, con=conn)
    return data
    
def get_term_x_ref_panda(conn):
    sql="SELECT ThesXrefs.TermID, ID, ThesXrefTypeID FROM ThesXrefs WHERE ThesXrefs.TableID=108 AND ThesXrefs.Active <> 0"
    data=pnd.read_sql(sql=sql, con=conn)
    return data

def get_terms_panda(conn):
    sql="SELECT TermID, Term, TermMasterID FROM Terms"
    data=pnd.read_sql(sql=sql, con=conn)
    return data

def get_termmaster_panda(conn):
    sql="SELECT TermMasterID, CN FROM TermMaster"
    data=pnd.read_sql(sql=sql, con=conn)
    return data

def get_constituent_pandas(conn, obj):
    sql="SELECT ConXrefDetails.ConstituentID, displayname, ConXrefs.RoleID FROM ConXrefDetails, ConXrefs, Constituents WHERE ConXrefs.TableID=108 AND ConXrefs.ID=? AND ConXrefs.ConXrefID = ConXrefDetails.ConXrefID AND ConXrefDetails.ConstituentID = Constituents.constituentid AND ConXrefDetails.UnMasked = 0"
    data=pnd.read_sql(sql=sql, con=conn, params=[obj])
    #print(data)
    return data
    
def get_constituent_panda_array(doc, pf, name_field ):
    values=[]
    for idx, row in pf.iterrows(): 
        if row["displayname"] is not None:
            values.append(row["displayname"])
    if len(values)>0:
        #print(name_field)
        #print(values)
        doc[name_field]=values
    
    

def create_doc(id, objnumber, sortnumber, title, medium, dimensions, acquisition_date, acquisition_method, short_description, creation_date):
    doc={
            "id": id,
            "object_number":objnumber.strip(),
            "sort_number":sortnumber.strip(),
            "title": title.strip(),
            "medium": medium.strip(),
            "dimensions": dimensions.strip(),
            "date_of_acquisition": acquisition_date.strip(),
            "acquisition_method": acquisition_method.strip(),
            "short_description": short_description.strip(),
            "creation_date":creation_date
        }
    
    #print(doc)
    return doc
    #solr.add(doc)
    #print(doc)

solr = pysolr.Solr(solr_url, always_commit=True)

cn = sa.create_engine('mssql+pyodbc://db/TMS?driver=ODBC Driver 17 for SQL Server')
cn_thesaurus = sa.create_engine('mssql+pyodbc://db/TMSThesaurus?driver=ODBC Driver 17 for SQL Server')


print("run")
now = datetime.datetime.now()
print ("Current date and time : ")
print (now.strftime("%Y-%m-%d %H:%M:%S"))
results_tms=cn.execute("with \
thes as (\
SELECT  [ThesXrefTypeID]\
      ,[ThesXrefType]\
      ,[TableID]\
      ,[ThesXrefTableID]\
      ,[MultiSelect]\
      ,[ArchiveDeletes]\
      ,[TermMasterID]\
      ,[ShowGuideTerms]\
      ,[BroadestTermFirst]\
      ,[NumLevels]\
      ,[LoginID]\
      ,[EnteredDate]\
      ,[sysTimeStamp]\
  FROM [TMS].[dbo].[ThesXrefTypes]\
),\
a as (\
SELECT  [FolderID]\
      ,[FolderName]\
      ,[FolderTypeID]\
      ,[Description]\
      ,[TableID]\
      ,[LoginID]\
      ,[EnteredDate]\
      ,[sysTimeStamp]\
  FROM [TMS].[dbo].[PackageFolders]\
  where lower([FolderName]) LIKE '%MIMO-2016%'), \
  b \
  as(\
\
  select b1.* from [TMS].[dbo].[PackageFolderXrefs] b1\
  INNER JOIN a ON a.[FolderID]=b1.[FolderID]),\
  c as\
  (\
  select c1.* from   [TMS].[dbo].[PackageList] c1\
  inner join b on b.[PackageID]=c1.[PackageID]\
  )\
  select DISTINCT d1.[ObjectID], d1.[ObjectNumber], d1.[SortNumber], [Title], [Medium], [Dimensions] , d1.[EnteredDate], [ObjAccession].[AccessionISODate], [AccessionMethods].[AccessionMethod], \
  [ObjContext].[ShortText8] \
  FROM [TMS].[dbo].[Objects] d1\
  INNER JOIN c ON d1.ObjectID=c.[ID]\
  LEFT JOIN ObjContext ON d1.ObjectID = ObjContext.ObjectID\
  LEFT JOIN [ObjAccession] ON d1.ObjectID = ObjAccession.ObjectID\
  LEFT JOIN [AccessionMethods] ON ObjAccession.AccessionMethodID = AccessionMethods.AccessionMethodID\
  ORDER BY d1.[EnteredDate] DESC;") 
rows = results_tms.fetchall() 
print("loading thes_x_ref")
pxref=get_term_x_ref_panda(cn)
print("thes_x_ref loaded")
print(len(pxref))
print("loading terms")
pterms=get_terms_panda(cn_thesaurus)
pterms2=pterms.copy()
print("terms loaded")
print(len(pterms))
print("loading termmasters")
ptermmaster=get_termmaster_panda(cn_thesaurus)
ptermmaster2=ptermmaster.copy()
print("termmaster loaded")
print(len(ptermmaster))
print("loading images")
pimages=get_images_panda(cn)
print("images loaded")
now = datetime.datetime.now()
print ("Current date and time : ")
print (now.strftime("%Y-%m-%d %H:%M:%S"))
i=0
for row in rows: 
    try:
        i=i+1
        #print(i)
        #print("-----")
        cn=test_connection(cn, 'mssql+pyodbc://db/TMS?driver=ODBC Driver 17 for SQL Server')
        if row is None:
            print("None row %s", i)
        #print("objid="+str(row[0]))
        if i% 500==0:
            print(i)
        doc=create_doc( row[0], row[1], row[2] or "", row[3] or "", row[4] or "", row[5] or "", row[7] or "", row[8] or "", row[9] or "", datetime.datetime.now().isoformat())
        
        pcontrib=get_constituent_pandas(cn,row[0] )
        '''
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==1], "cons_maker")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==2], "cons_expeditor")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==5], "cons_previous_owner")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==16], "field_seller")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==18], "field_collector")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==19], "cons_mission")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==20], "cons_identification")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==23], "cons_image")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==24], "cons_photographer")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==26], "cons_client")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==29], "cons_original_owner")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==29], "cons_original_owner")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==34], "cons_exchange")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==35], "cons_intermediate")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==40], "cons_publisher")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==41], "cons_engraver")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==84], "cons_excol_collector")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==85], "cons_excol_donor")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==86], "cons_excol_exchange")
        get_constituent_panda_array(doc, pcontrib.loc[(pcontrib["RoleID"]==87)| (pcontrib["RoleID"]==35)], "cons_excol_intermediary")
        get_constituent_panda_array(doc, pcontrib.loc[(pcontrib["RoleID"]==88) | (pcontrib["RoleID"]==30)], "cons_excol_legator")
        get_constituent_panda_array(doc, pcontrib.loc[(pcontrib["RoleID"]==89) | (pcontrib["RoleID"]==29)], "cons_excol_lender")
        get_constituent_panda_array(doc, pcontrib.loc[(pcontrib["RoleID"]==90) | (pcontrib["RoleID"]==19)], "cons_excol_mission")
        get_constituent_panda_array(doc, pcontrib.loc[(pcontrib["RoleID"]==91) | (pcontrib["RoleID"]==21)], "cons_excol_owner")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==92], "cons_excol_transfer")
        get_constituent_panda_array(doc, pcontrib.loc[(pcontrib["RoleID"]==93)| (pcontrib["RoleID"]==31)], "cons_excol_unknown")
        get_constituent_panda_array(doc, pcontrib.loc[(pcontrib["RoleID"]==94)| (pcontrib["RoleID"]==16)], "cons_excol_vendor")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==96], "cons_excol_field_collector")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==100], "cons_excol_depositor")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==108], "cons_excol_excavation_by")
        get_constituent_panda_array(doc, pcontrib.loc[pcontrib["RoleID"]==109], "cons_excol_prospection_by")
        get_constituent_panda_array(doc, pcontrib.loc[(pcontrib["RoleID"]==112) | (pcontrib["RoleID"]==27)], "cons_excol_depot")
        '''
        get_site_of_collection(doc, cn,row[0])
        get_thesaurus_term(doc, pxref, pterms, ptermmaster,  (pxref["ID"]==row[0]) & (pxref["ThesXrefTypeID"]==46), "geo_production")
        get_thesaurus_term(doc, pxref, pterms, ptermmaster,  (pxref["ID"]==row[0]) & (pxref["ThesXrefTypeID"]==44), "culture")
        get_thesaurus_term(doc, pxref, pterms, ptermmaster,  (pxref["ID"]==row[0]) & (pxref["ThesXrefTypeID"]==49), "culture_of_production")
        #get_thesaurus_term_hierarchy(doc, pxref, pterms, ptermmaster, pterms2, ptermmaster2, (pxref["ID"]==row[0]) & (pxref["ThesXrefTypeID"]==44), "culture")
        #get_thesaurus_term_hierarchy(doc, pxref, pterms, ptermmaster, pterms2, ptermmaster2, (pxref["ID"]==row[0]) & (pxref["ThesXrefTypeID"]==49), "culture_of_production")
        
        get_titles(cn, doc, row[0], 10, "objtitle_legacy" )
        get_titles(cn, doc, row[0], 6, "objtitle_vernacular" )
        
        get_images(doc,pimages, (pimages["ObjectId"]==row[0]) & (pimages["PrimaryDisplay"]==1), "images_primary")
        get_images(doc,pimages, (pimages["ObjectId"]==row[0]) & (pimages["PrimaryDisplay"]!=1), "images_non_primary")
        
        handle_images(cn, doc, row[0])
        
        #print(row)
        #create_doc(solr, row[0], row[1], row[2] or "", row[3] or "", row[4] or "", row[5] or "", datetime.datetime.now().isoformat())
        solr.add([doc])
    except BaseException as ex:
        # Get current system exception
        print("Exception line %s", str(i))
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
        print("Exception type : %s " % ex_type.__name__)
        print("Exception message : %s" %ex_value)
        print("Stack trace : %s" %stack_trace)


#cn.close()
#cursor_thesaurus.close()       
print("done")
print(i)
now = datetime.datetime.now()
print ("Current date and time : ")
print (now.strftime("%Y-%m-%d %H:%M:%S"))
cn.dispose()
