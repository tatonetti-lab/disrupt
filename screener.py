"""

to-do: take only HIGHEST stage per patient, adjust possibly progressed to limit to ones since most recent appointment, CT/MRI/PET (check Briane's notes). Re-examine Ruby data fo tigure out why bad stages

screener.py

VERSION: 0.1

INPUTS
======
- list of oncology departments

OUTPUTS
=======
- tabular data of patients with Structured Elegibility Criteria (SEC)

STEPS
=====
1. Identify patients
    a. using the list of oncology deparmtnets, find patients with appointments
       within the next [2] weeks
    b. any patient with a `first cancer diagonsis` within the previous [4] weeks
        - `first cancer diagnosis` is first known to us
    {c. patients who progressed or non-responsive to therapy}
        - this one is left to implement in version 0.2

2. Clinical Data Extraction
    a. extract Cancer Type by OMOP Concept IDs (mapped back to clarity)
        - will need a script to generate the map back to clarity (ICD10)
          and this will be updated each time OMOP is updated
        i. Breast: 4112853 (note - for ICD-10 this is just C50%)
        ii. Prostate: 4163261 (note - for ICD-10 this is just C61%)
        iii. Liver: 4246127 (note - for ICD-10 this is just C22%)
    b. extract Stage through text processing
        - starting with Breast (well structured) identify TNM staging and map
          to format defined in the SEC
        - Prostate and Liver (TODO)
    c. extract Receptor status through text processing
        - starting with Breast (well structured) and map to SEC
        - Prostate and Liver (TODO)
    {d. line of therapy (TODO: only applicable to progressing/recurrent patients)}
        - using historical pharmacy orders we can infer the line of therapy
        - map this line of therapy to the SEC

3. Compile the output data and write to a file/database store
    a. tabulary data where each row is a patient
    b. columns:
        - mrn
        - cancer_type
        - stage
        - receptor_status
        - appointment onology department
        - first cancer diagnosis date
        - most recent cancer diagnosis date
        - prior_treatment {flag? description? not sure} (currently only taking yes/no known exposure to anti-neoplastic agents on the basis of order_med)

TO-DO
=======
Add some data for the patient regarding provinance (ie "this patient showed up because they had receptor status" etc)

NOTE: EXPORT JAVA_HOME FROM LOCAL ENVIRONMENT TO POINT TO JAVA JDK /bin

add argparse to deal with command line switches

move all SQL statements into config file

pass in config file as command line argument

add a switch as to whether to re-populate the tables or not.

"""

import pyodbc
import pandas as pd
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
import numpy as np
import re
import os
import sqlite3

sqlite = sqlite3.connect('disrupt.db')

path2note=r"pathtonotes"
path2image= r"pathtoimagingnotes"
print(path2note)
print(path2image)

df_image = pd.read_csv(path2image,encoding='utf8')

df_path = pd.read_csv(path2note,encoding='utf8')

df_path.sort_values(by='SERVICE_DATE',inplace=True, ascending= False)

df_path = df_path.fillna('')
print('num of records: ',len(df_path))
print('unique patients: ',df_path.MRN.nunique())

df_path.reset_index(inplace = True)
df_path.drop('index', axis=1,inplace = True)
df_path.head(3)



def find_cancer(note_text):
    #print(note_text['MRN'])
    m = re.findall('((NEW|New))',str(note_text))
    s=''
    if len(m)>0 :
        for item in m:
            s = s+item[0]+','
    if len(s)>0:
        #print(s)
        return "NEW"
    else :
        return "PROGRESSED"
    
    return np.nan

df_path['MATCH TYPE'] = df_path['MATCH_TYPE'].apply(find_cancer)

def find_breast_cancer(note_text):
    #print(note_text['MRN'])
    m = re.findall('((Breast cancer|breast cancer)|(metastatic carcinoma consistent by history and morphology with mammary ductal carcinoma)|(adenocarcinoma compatible with breast origin)|(malignant neoplasm of right female breast)|(malignant neoplasm of left female breast)|(left breast cancer)|(left breast carcinoma)|(history left breast cancer)|(recent diagnosis of left breast cancer)|(Breast CA|breast CA)|(DCIS)|(ductal carcinoma in situ|ductal carcinoma in situ (DCIS)))',str(note_text))
    s=''
    if len(m)>0 :
        for item in m:
            s = s+item[0]+','
    if len(s)>0:
        #print(s)
        return "Breast"
        
    return np.nan

df_path['Cancer type'] = df_path['TEXT'].apply(find_breast_cancer)


def find_match_all(note_text):
    m = re.findall('([a-zA-Z0-9]*([Tt][Xx0-4](is|IS)*)(p|c)*([Nn][Xx0-3][a-dA-D]{0,1})*(p|c)*[Mm][0-1xX]*[a-zA-Z0-9]*)',str(note_text))
    s=''
    if len(m)>0 :
        for i in m:
            s = s+i[0]+' '
    if len(s)>0:
        #print(s)
        return s
    return None


def find_match_recep_stat(note_text):
    recep_stat = []
    m = re.findall('[/ \(]((HER2|ER|PR)[ \-+](\w*))',str(note_text))
    if m is not None:
        her2 =None
        er = None
        pr=None
        for match in m:
                #print(match)
                if match[1] == 'HER2' and not 'Neu' in match[0]:
                        if 'neg' in match[2].lower():
                        #if 'neg' in match[0].lower():
                                her2 = 'HER2-'
                        elif 'pos' in match[2].lower():
                        #elif 'pos' in match[0].lower():
                                her2 = 'HER2+'
                        elif '+' in match[0].lower():
                                her2 = 'HER2+'
                        elif '-' in match[0].lower() and match[2] =='':
                        #elif '-' in match[0].lower():
                                her2 = 'HER2-'
                if match[1] == 'ER':
                        if 'neg' in match[2].lower():
                        #if 'neg' in match[0].lower():
                                er = 'ER-'
                        elif 'pos' in match[2].lower():
                        #elif 'pos' in match[0].lower():
                                er = 'ER+'
                        elif '+' in match[0].lower():
                                er = 'ER+'
                        elif '-' in match[0].lower() and match[2] == '':
                        #elif '-' in match[0].lower():
                                er = 'ER-'

                if match[1] == 'PR':
                        if 'neg' in match[2].lower():
                        #if 'neg' in match[0].lower():
                                pr = 'PR-'
                        elif 'pos' in match[2].lower():
                        #elif 'pos' in match[0].lower():
                                pr = 'PR+'
                        elif '+' in match[0].lower():
                                pr = 'PR+'
                        elif '-' in match[0].lower() and match[2] =='':
                        #elif '-' in match[0].lower():
                                pr = 'PR-'
#                 if(her2 != ''):
#                         return her2
#                 if(er != ''):
#                         return er
#                 if(pr != ''):
#                         return pr
#     recep_stat = {}
#     recep_stat['ER'] = er
#     recep_stat['PR'] = pr
#     recep_stat['Her2'] =her2
    recep_stat.append(er)
    recep_stat.append(pr)

    recep_stat.append(her2)
#     print(recep_stat)
    return recep_stat


df_path['recep_stat'] = df_path['TEXT'].apply(find_match_recep_stat)



def find_match_m(note_text):
    #print(note_text['MRN'])
#     m = re.findall('([a-zA-Z0-9]*([Tt][Xx0-4]([a-dA-D]|is|IS)*))',str(note_text))
#     m = re.findall('((p|c)*([Nn][Xx0-3][a-dA-D]{0,1}))',str(note_text))
#     m = re.findall('((p|c)*([Mm][0-1xX]+[\(i+\)]*))',str(note_text))
    m = re.findall('(((p|c)*([M][0-1][X]*[\(i+\)]*\s))|((Metastatic|metatastic)\s*Breast\s*Cancer)|(MBC)|((Metastatic|metatastic) TNBC)|([Mm]etastatic))',str(note_text))
#     m = re.findall('((p|c)*([Mm][0-1][xX]{0,1}+[\(i+\)]*))',str(note_text))
    #print(m)
    s=''
    if len(m)>0 :
        for i in m:
            s = s+i[0]+' '
    if len(s)>0:
        #print(s)
        return s
        
    return None



def find_match_n(note_text):
    m = re.findall('((p|c)*([N][0-3][a-dA-D]{0,1}))',str(note_text))

    s=''
    if len(m)>0 :
        for i in m:
            s = s+i[0]+' '
    if len(s)>0:
        #print(s)
        return s
    return None



def find_match_t(note_text):
    #print(note_text['MRN'])
    m = re.findall('(([y]*(p|c)*[T][0-4]([abc]|is|IS)*))',str(note_text))
#     m = re.findall('([Tt][Xx0-4]([a-bA-b]|is|IS)*)',str(note_text))
#     print(type(m))
    s=''
    if len(m)>0:
        for i in m:
            s = s+i[0]+' '
    else:
        m = re.findall('([y]*(p|c)*[T](is|IS))',str(note_text))
        #print(m)
        for i in m:
            s = s+i[0]+' '
        
        
    if len(s)>0:
        #print(s)
        return s
        
    return None


df_path['T'] = df_path['TEXT'].apply(find_match_t)
df_path['N'] = df_path['TEXT'].apply(find_match_n)
df_path['M'] = df_path['TEXT'].apply(find_match_m)
df_path['all'] = df_path['TEXT'].apply(find_match_all)
df_path['recep_stat'] = df_path['TEXT'].apply(find_match_recep_stat)
df_path['ER'] = df_path['recep_stat'].apply(lambda x: x[0])
df_path['PR'] = df_path['recep_stat'].apply(lambda x: x[1])
df_path['Her2'] = df_path['recep_stat'].apply(lambda x: x[2])
df_path['Cancer Type'] = df_path['TEXT'].apply(find_breast_cancer)
df_path['MATCH TYPE'] = df_path['MATCH_TYPE'].apply(find_cancer)
print(len(df_path))
#df_path.dropna(subset=['T','N','M','all','recep_stat'],inplace=True)
print(len(df_path))
df_path.reset_index(inplace = True)
df_path.head(2)

#df_path = df_path[['MRN', 'SERVICE_DATE', 'TYPE', 'TEXT', 'TEXT+DATE', 'T', 'N','M','all','ER','PR','Her2','Cancer Type','MATCH TYPE']]
#mrn_path = list(df_path['MRN'].unique())

def extract_staging(txt):
    
    M = txt[0]
    T = txt[1]
    N = txt[2]
    total = txt[3]
    #print('\n',M,T,N)
    #print(total)
    #print(~np.isnan(M))
    overallstage = ''
    if (M is not None):
        if 'm1' in M.lower() or 'metastatic' in M.lower():
        #if 'm1' in M.lower() in M.lower():
                overallstage = 'Stage IV'
                return overallstage
    if total is not None :
        if 'm1' in total.lower():
            overallstage = 'Stage IV'
            return overallstage
    if T is not None and N is not None:
        if 'tis' in T.lower() and 'n0' in N.lower():
                overallstage = 'Stage 0'
                return overallstage
        elif 't1' in T.lower() and 'n0' in N.lower():
                overallstage = 'Stage 1A'
                return overallstage
        elif 't0' in T.lower() and 'n1mi' in N.lower():
                overallstage = 'Stage 1B'
                return overallstage
        elif 't1' in T.lower() and 'n1mi' in N.lower():
                overallstage = 'Stage 1B'
                return overallstage
        elif 't0' in T.lower() and 'n1' in N.lower():
                overallstage = 'Stage IIA'
                return overallstage
        elif 't1' in T.lower() and 'n1' in N.lower():
                overallstage = 'Stage IIA'
                return overallstage
        elif 't2' in T.lower() and 'n0' in N.lower():
                overallstage = 'Stage IIA'
                return overallstage
        elif 't2' in T.lower() and 'n1' in N.lower():
                overallstage = 'Stage IIB'
                return overallstage
        elif 't3' in T.lower() and 'n0' in N.lower():
                overallstage = 'Stage IIB'
                return overallstage
        elif 't0' in T.lower() and 'n2' in N.lower():
                overallstage = 'Stage IIIA'
                return overallstage
        elif 't1' in T.lower() and 'n2' in N.lower():
                overallstage = 'Stage IIIA'
                return overallstage
        elif 't2' in T.lower() and 'n2' in N.lower():
            overallstage = 'Stage IIIA'
            return overallstage
        elif 't3' in T.lower() and 'n1' in N.lower():
            overallstage = 'Stage IIIA'
            return overallstage
        elif 't3' in T.lower() and 'n2' in N.lower():
            overallstage = 'Stage IIIA'
            return overallstage
        elif 't4' in T.lower() and 'n0' in N.lower():
            overallstage = 'Stage IIIB'
            return overallstage
        elif 't4' in T.lower() and 'n1' in N.lower():
            overallstage = 'Stage IIIB'
            return overallstage
        elif 't4' in T.lower() and 'n2' in N.lower():
            overallstage = 'Stage IIIB'
            return overallstage
    if N is not None:
        if 'n3' in N.lower():
            overallstage = 'Stage IIIC'
            return overallstage
    if total is not None:
        if 'tis' in total.lower() and 'n0' in total.lower():
            overallstage = 'Stage 0'
            return overallstage
        elif 't1' in total.lower() and 'n0' in total.lower():
                overallstage = 'Stage 1A'
                return overallstage
        elif 't0' in total.lower() and 'n1mi' in total.lower():
                overallstage = 'Stage 1B'
                return overallstage
        elif 't1' in total.lower() and 'n1mi' in total.lower():
                overallstage = 'Stage 1B'
                return overallstage
        elif 't0' in total.lower() and 'n1' in total.lower():
                overallstage = 'Stage IIA'
                return overallstage
        elif 't1' in total.lower() and 'n1' in total.lower():
                overallstage = 'Stage IIA'
                return overallstage
        elif 't2' in total.lower() and 'n0' in total.lower():
                overallstage = 'Stage IIA'
                return overallstage
        elif 't2' in total.lower() and 'n1' in total.lower():
                overallstage = 'Stage IIB'
                return overallstage
        elif 't3' in total.lower() and 'n0' in total.lower():
                overallstage = 'Stage IIB'
                return overallstage
        elif 't0' in total.lower() and 'n2' in total.lower():
                overallstage = 'Stage IIIA'
        elif 't1' in total.lower() and 'n2' in total.lower():
                overallstage = 'Stage IIIA'
                return overallstage
        elif 't2' in total.lower() and 'n2' in total.lower():
            overallstage = 'Stage IIIA'
            return overallstage
        elif 't3' in total.lower() and 'n1' in total.lower():
            overallstage = 'Stage IIIA'
        elif 't3' in total.lower() and 'n2' in total.lower():
            overallstage = 'Stage IIIA'
            return overallstage
        elif 't4' in total.lower() and 'n0' in total.lower():
            overallstage = 'Stage IIIB'
        elif 't4' in total.lower() and 'n1' in total.lower():
            overallstage = 'Stage IIIB'
            return overallstage
        elif 't4' in total.lower() and 'n2' in total.lower():
            overallstage = 'Stage IIIB'
            return overallstage
        elif 'n3' in N.lower():
            overallstage = 'Stage IIIC'
            return overallstage
    elif T is not None:
        overallstage = 'NOT ENOUGH INFORMATION'        
    else:
        overallstage = 'NOT ENOUGH INFORMATION'
    return overallstage
        
    
dft = df_path.dropna(subset = ['T'], axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['T'].unique().to_frame()
dft.reset_index(inplace=True)
# # df_com.TEXT = df.TEXT.apply(list)
dft['T'] = dft['T'].apply(lambda x: x[0].split(' ')[0])
print(len(dft))
# print(df2['MRN'].iloc[0])
dft.head(2)

dfm = df_path.dropna(subset = ['M'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['M'].unique().to_frame()
dfm.reset_index(inplace=True)
print(len(dfm))
dfm['M'] = dfm['M'].apply(lambda x: x[0].split(' ')[0])
# print(df2['MRN'].iloc[0])
dfm.head(2)

dfn = df_path.dropna(subset=['N'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['N'].unique().to_frame()
dfn.reset_index(inplace=True)
dfn['N'] = dfn['N'].apply(lambda x: x[0].split(' ')[0])
print(len(dfn))
# print(df2['MRN'].iloc[0])
dfn.head(2)

df_all = df_path.dropna(subset=['all'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['all'].unique().to_frame()
df_all.reset_index(inplace=True)
df_all['all'] = df_all['all'].apply(lambda x: x[0].split(' ')[0])
print(len(df_all))
# print(df2['MRN'].iloc[0])
df_all.head(2)


df_PR = df_path.dropna(subset=['PR'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['PR'].unique().to_frame()
df_PR.reset_index(inplace=True)
print(len(df_PR))
df_PR['PR'] = df_PR['PR'].apply(lambda x: x[0])
# df_recep.recep_stat = df_recep.recep_stat
# print(df2['MRN'].iloc[0])
df_PR.head(2)


df_ER = df_path.dropna(subset=['ER'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['ER'].unique().to_frame()
df_ER.reset_index(inplace=True)
print(len(df_ER))
df_ER['ER'] = df_ER['ER'].apply(lambda x: x[0])
# df_recep.recep_stat = df_recep.recep_stat
# print(df2['MRN'].iloc[0])
df_ER.head(2)


df_her = df_path.dropna(subset=['Her2'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['Her2'].unique().to_frame()
df_her.reset_index(inplace=True)
print(len(df_her))
df_her['Her2'] = df_her['Her2'].apply(lambda x: x[0])
# df_recep.recep_stat = df_recep.recep_stat
# print(df2['MRN'].iloc[0])
df_her.head(2)


df_ct = df_path.dropna(subset=['Cancer Type'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['Cancer Type'].unique().to_frame()
df_ct.reset_index(inplace=True)
print(len(df_ct))
df_ct['Cancer Type'] = df_ct['Cancer Type'].apply(lambda x: x[0])
# df_recep.recep_stat = df_recep.recep_stat
# print(df2['MRN'].iloc[0])
df_ct.head(2)


df_c = df_path.dropna(subset=['MATCH TYPE'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['MATCH TYPE'].unique().to_frame()
df_c.reset_index(inplace=True)
print(len(df_c))
df_c['MATCH TYPE'] = df_c['MATCH TYPE'].apply(lambda x: x[0])
# df_recep.recep_stat = df_recep.recep_stat
# print(df2['MRN'].iloc[0])
df_c.head(2)


df_f = pd.merge(dfm,dfn,on='MRN',how='outer')
print(len(df_f))
df_f = pd.merge(df_f,dft,on='MRN',how='outer')
print(len(df_f))
df_f.head(2)
df_f = pd.merge(df_f,df_all,on='MRN',how='outer')
df_f = pd.merge(df_f,df_ER,on='MRN',how='outer')
df_f = pd.merge(df_f,df_PR,on='MRN',how='outer')
df_f = pd.merge(df_f,df_her,on='MRN',how='outer')
df_f = pd.merge(df_f,df_ct,on='MRN',how='outer')
df_f = pd.merge(df_f,df_c,on='MRN',how='outer')
#df_f = pd.merge(df_f,df_recep,on='MRN',how='outer')
df_f = df_f.where(pd.notnull(df_f), None)
print(len(df_f))
df_f.head(3)

df_f ['Overall stage'] = df_f[['M','T','N','all']].apply(extract_staging,axis=1)

set(df_f['all'].values)

pks = dict()
sqlite_cursor = sqlite.cursor()

print("processing results into sqlite")
if df_f is not None:
        for ind in df_f.index:
                #sqlite_cursor.execute(f"insert into patient (pat_id, mrn, dob, cancer_type, new_or_progressed, date_screened) values ('{pat_id}','{mrn}','{dob}','Breast','{match_type}','{datetime.now()}')")
                sqlite_cursor.execute("insert into patient (pat_id, mrn, dob, cancer_type, new_or_progressed, date_screened) values ('%(pat_id)s','%(mrn)s','%(dob)s','%(disease)s','%(match_type)s','%(now)s')" % {'disease': df_f['Cancer Type'][ind],'pat_id': ind, 'mrn': df_f['MRN'][ind], 'dob': '1/1/1975', 'match_type': df_f['MATCH TYPE'][ind], 'now': datetime.now()})
                #pks[ind] = sqlite_cursor.lastrowid
                sqlite_cursor.execute ("insert into patient_staging (fk_id, stage, stage_t, stage_n, stage_m) values (%(pat_id)s,'%(stage)s','%(T)s','%(N)s','%(M)s')" % {'pat_id': ind, 'stage': df_f['Overall stage'][ind], 'T': df_f['T'][ind], 'N': df_f['N'][ind], 'M': df_f['M'][ind]})
                sqlite_cursor.execute ("insert into patient_receptor values ('%(pat_id)s','%(key)s','%(value)s')" % {'pat_id': ind, 'key': 'ER', 'value': df_f['ER'][ind]})
                sqlite_cursor.execute ("insert into patient_receptor values ('%(pat_id)s','%(key)s','%(value)s')" % {'pat_id': ind, 'key': 'PR', 'value': df_f['PR'][ind]}) 
                sqlite_cursor.execute ("insert into patient_receptor values ('%(pat_id)s','%(key)s','%(value)s')" % {'pat_id': ind, 'key': 'HER2', 'value': df_f['Her2'][ind]})
    
    
sqlite.commit()
sqlite.close()
today = datetime.today()
print(today.strftime("%Y/%m/%d %H:%M:%S"))
