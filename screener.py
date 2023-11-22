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
from datetime import datetime


def find_status(note_text):
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


def find_breast_cancer(note_text):
    m = re.findall('((Breast cancer|breast cancer)|(metastatic carcinoma consistent by history and morphology with mammary ductal carcinoma)|(adenocarcinoma compatible with breast origin)|(malignant neoplasm of right female breast)|(malignant neoplasm of left female breast)|(left breast cancer)|(left breast carcinoma)|(history left breast cancer)|(recent diagnosis of left breast cancer)|(Breast CA|breast CA)|(DCIS)|(ductal carcinoma in situ|ductal carcinoma in situ (DCIS)))',str(note_text))
    s=''
    if len(m)>0 :
        for item in m:
            s = s+item[0]+','
    if len(s)>0:
        return "Breast"
        
    return np.nan

def find_stage_cancer(note_text):
    overallstage = ''
    metastc = re.findall('((metastatic breast cancer))',str(note_text),re.IGNORECASE)
    clinic_stage = re.findall('(Stage[^.;\/]*\s+(?:I(?:[AB])? |1(?:[AB])? |II(?:[AB])? |III(?:[AB]?C)? |IV ))', str(note_text))
    m = re.search('.*AJCC ([0-9]{1,2}[a-z][a-z]) Edition.+?(Clinical|Pathologic):*.*(Stage (?:I(?:[AB])?|II(?:[AB])?|III(?:[AB]?C)?|IV) \(.*?\)*.)', str(note_text))
    
    if m is not None:
        ajcc = m.group(1)
        path_or_clin = m.group(2)
        staging = m.group(3)
        openparen = staging.index('(')
        closeparen = staging.rfind(')')
        overallstage = staging[0:(openparen-1)];
        staging_str = staging[(openparen+1):closeparen]
    if metastc:
        overallstage = 'Stage IV'
        return overallstage
    if len(overallstage)>0:
        #print(s)
        return overallstage
    elif len(overallstage)<=0 and clinic_stage:
        overallstage = clinic_stage[-1]
        return overallstage
        
    return None

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
    #print(note_text['MRN'])
    recep_stat = []
    m = re.findall('[/ \(]((HER2|ER|PR)[ \-+](\w*))',str(note_text))
    #m = re.findall('([/ \(](HER2|ER|PR)[ \-+]([PpNn]\w*)*)',str(note_text))
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

                            
                            
    
    ######    Sign search
    status_patterns = re.findall(r'(?:pos|neg|\+|-)/(?:pos|neg|\+|-)/(?:pos|neg|\+|-)', note_text)
    
    # Initialize status variables
    er_sign_status = pr_sign_status = her2_sign_status = last_pattern = ''
    if status_patterns:
        last_pattern = status_patterns[-1].split("/")
        
        if last_pattern[0] == 'pos' or last_pattern[0] == '+':
            er_sign_status = 'ER+'
        elif last_pattern[0] == 'neg' or last_pattern[0] == '-':
            er_sign_status = 'ER-'

        if last_pattern[1] == 'pos' or last_pattern[1] == '+':
            pr_sign_status = 'PR+'
        elif last_pattern[1] == 'neg' or last_pattern[1] == '-':
            pr_sign_status = 'PR-'

        if last_pattern[2] == 'pos' or last_pattern[2] == '+':
            her2_sign_status = 'HER2+'
        elif last_pattern[2] == 'neg' or last_pattern[2] == '-':
            her2_sign_status = 'HER2-'


      
    
    
    
    ######  Word search
    er_status = pr_status = her2_status = ''
    er_text = pr_text = her2_text = ''
    # Use re.findall to find ER status
    er_statuses = re.findall(r'(?<!\w)(?:ER:|ER |ER :|ESTROGEN RECEPTOR)[^.;]*\s*(?:\(\s*POSITIVE\s*\)|\(\s*NEGATIVE\s*\)|\s*POSITIVE\s*|\s*NEGATIVE\s*)', note_text, re.IGNORECASE)
    
    # Use re.findall to find PR status
    pr_statuses = re.findall(r'(?<!\w)(?:PR:|PR |PR :|PROGESTERONE RECEPTOR)[^;]*\s*(?:\(\s*POSITIVE\s*\)|\(\s*NEGATIVE\s*\)|\s*POSITIVE\s*|\s*NEGATIVE\s*)', note_text, re.IGNORECASE)
    
    # Use re.findall to find HER2 status
    her2_statuses = re.findall(r'HER[ -]?2:?[^;]*\s*(?:\(\s*POSITIVE\s*\)|\(\s*NEGATIVE\s*\)|\s*POSITIVE\s*|\s*NEGATIVE\s*)', note_text, re.IGNORECASE)

    if er_statuses:
        er_status = er_statuses[-1]  # Take the last ER status found

    if pr_statuses:
        pr_status = pr_statuses[-1]  # Take the last PR status found

    if her2_statuses:
        her2_status = her2_statuses[-1]  # Take the last HER2 status found

    if 'negative' in er_status.lower():
        er_text = "ER-"
    elif 'positive' in er_status.lower():
        er_text = "ER+"

    if 'negative' in pr_status.lower():
        pr_text = "PR-"
    elif 'positive' in pr_status.lower():
        pr_text = "PR+"

    if 'negative' in her2_status.lower():
        her2_text = "HER2-"
    elif 'positive' in her2_status.lower():
        her2_text = "HER2+"
        
    neg_statuses = re.findall(r'(?:TNBC |TNBC;|TNBC.|TNBC,)', note_text, re.IGNORECASE)
    if neg_statuses:
        er_text = "ER-"
        pr_text = "PR-"
        her2_text = "HER2-"
        
        
    #####Final solution
    
    if er_sign_status and pr_sign_status and her2_sign_status:
        er = er_sign_status
        pr = pr_sign_status
        her2 = her2_sign_status
        
        recep_stat.append(er)
        recep_stat.append(pr)
        recep_stat.append(her2)
        
        return recep_stat
    
    textcount = 0
    if er_text and pr_text and her2_text:
        if er_text:
            er = er_text
            textcount+=1
        if pr_text:
            pr = pr_text
            textcount+=1
        if her2_text:
            her2 = her2_text
            textcount+=1
        if textcount == 3:
            recep_stat.append(er)
            recep_stat.append(pr)
            recep_stat.append(her2)
            return recep_stat
    
    
    recep_stat.append(er)
    recep_stat.append(pr)

    recep_stat.append(her2)
#     print(recep_stat)
    return recep_stat



def find_match_m(note_text):
    m = re.findall('(((p|c)*([M][0-1][X]*[\(i+\)]*\s))|((Metastatic|metatastic)\s*Breast\s*Cancer)|(MBC)|((Metastatic|metatastic) TNBC)|([Mm]etastatic))',str(note_text))
    s=''
    if len(m)>0 :
        for i in m:
            s = s+i[0]+' '
    if len(s)>0:
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
    m = re.findall('(([y]*(p|c)*[T][0-4]([abc]|is|IS)*))',str(note_text))
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



def extract_staging(txt):
    
    M = txt[0]
    T = txt[1]
    N = txt[2]
    total = txt[3]
    ajcc = txt[4]
    overallstage = ''
    if(ajcc is not None):
        overallstage = ajcc
    elif(ajcc is None):
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
        
    

if __name__ == '__main__':
    
    #Connect to the database
    sqlite = sqlite3.connect('disrupt.db')

    #Path to the files
    path2note=r"pathtonotes"
    path2cohort= r"pathtocohortnotes"
    
    #Reading files
    df_coh_list = pd.read_csv(path2cohort,encoding='utf8')
    df_path = pd.read_csv(path2note,encoding='latin-1')
    

    df_path.sort_values(by='SERVICE_DATE',inplace=True, ascending= False)
    df_path = df_path.fillna('')
    print('num of records: ',len(df_path))
    print('unique patients: ',df_path.MRN.nunique())
    df_path.reset_index(inplace = True)
    df_path.drop('index', axis=1,inplace = True)
    df_path.head(3)
    
    #Applying functions
    df_path['T'] = df_path['TEXT'].apply(find_match_t)
    df_path['N'] = df_path['TEXT'].apply(find_match_n)
    df_path['M'] = df_path['TEXT'].apply(find_match_m)
    df_path['all'] = df_path['TEXT'].apply(find_match_all)
    df_path['recep_stat'] = df_path['TEXT'].apply(find_match_recep_stat)
    df_path['ER'] = df_path['recep_stat'].apply(lambda x: x[0])
    df_path['PR'] = df_path['recep_stat'].apply(lambda x: x[1])
    df_path['Her2'] = df_path['recep_stat'].apply(lambda x: x[2])
    df_path['AJCC'] = df_path['TEXT'].apply(find_stage_cancer)
    df_path['Cancer Type'] = df_path['TEXT'].apply(find_breast_cancer)
    df_path['MATCH TYPE'] = df_path['MATCH_TYPE'].apply(find_status)
    df_path.reset_index(inplace = True)
    df_path.head(2)
    
    df_path = df_path[['MRN', 'MATCH TYPE', 'Cancer Type', 'SERVICE_DATE', 'TYPE', 'TEXT', 'T', 'N','M','all','ER','PR','Her2','AJCC']]

    
    dft = df_path.dropna(subset = ['T'], axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['T'].unique().to_frame()
    dft.reset_index(inplace=True)
    dft['T'] = dft['T'].apply(lambda x: x[0].split(' ')[0])
    dft.head(2)

    dfm = df_path.dropna(subset = ['M'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['M'].unique().to_frame()
    dfm.reset_index(inplace=True)
    print(len(dfm))
    dfm['M'] = dfm['M'].apply(lambda x: x[0].split(' ')[0])
    dfm.head(2)

    dfn = df_path.dropna(subset=['N'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['N'].unique().to_frame()
    dfn.reset_index(inplace=True)
    dfn['N'] = dfn['N'].apply(lambda x: x[0].split(' ')[0])
    print(len(dfn))
    dfn.head(2)

    df_all = df_path.dropna(subset=['all'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['all'].unique().to_frame()
    df_all.reset_index(inplace=True)
    df_all['all'] = df_all['all'].apply(lambda x: x[0].split(' ')[0])
    print(len(df_all))
    df_all.head(2)
    
    df_st = df_path.dropna(subset=['AJCC'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['AJCC'].unique().to_frame()
    df_st.reset_index(inplace=True)
    print(len(df_st))
    df_st['AJCC'] = df_st['AJCC'].apply(lambda x: x[0])
    df_st.head(5)


    df_PR = df_path.dropna(subset=['PR'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['PR'].unique().to_frame()
    df_PR.reset_index(inplace=True)
    print(len(df_PR))
    df_PR['PR'] = df_PR['PR'].apply(lambda x: x[0])
    df_PR.head(2)


    df_ER = df_path.dropna(subset=['ER'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['ER'].unique().to_frame()
    df_ER.reset_index(inplace=True)
    print(len(df_ER))
    df_ER['ER'] = df_ER['ER'].apply(lambda x: x[0])
    df_ER.head(2)


    df_her = df_path.dropna(subset=['Her2'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['Her2'].unique().to_frame()
    df_her.reset_index(inplace=True)
    print(len(df_her))
    df_her['Her2'] = df_her['Her2'].apply(lambda x: x[0])
    df_her.head(2)


    df_ct = df_path.dropna(subset=['Cancer Type'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['Cancer Type'].unique().to_frame()
    df_ct.reset_index(inplace=True)
    print(len(df_ct))
    df_ct['Cancer Type'] = df_ct['Cancer Type'].apply(lambda x: x[0])
    df_ct.head(2)


    df_c = df_path.dropna(subset=['MATCH TYPE'],axis=0).drop_duplicates(subset = 'MRN', keep='first').groupby(['MRN'])['MATCH TYPE'].unique().to_frame()
    df_c.reset_index(inplace=True)
    print(len(df_c))
    df_c['MATCH TYPE'] = df_c['MATCH TYPE'].apply(lambda x: x[0])
    df_c.head(2)

    #Merging all data
    df_f = pd.merge(dfn,dfm,on='MRN',how='outer')
    df_f = pd.merge(df_f,dft,on='MRN',how='outer')
    df_f = pd.merge(df_f,df_all,on='MRN',how='outer')
    df_f = pd.merge(df_f,df_ER,on='MRN',how='outer')
    df_f = pd.merge(df_f,df_PR,on='MRN',how='outer')
    df_f = pd.merge(df_f,df_her,on='MRN',how='outer')
    df_f = pd.merge(df_f,df_st,on='MRN',how='outer')
    df_f = pd.merge(df_f,df_c,on='MRN',how='outer')
    df_f = pd.merge(df_f,df_ct,on='MRN',how='outer')
    df_f = pd.merge(df_f, df_coh_list[['MRN', 'BIRTHDATE']], on='MRN', how='outer')
    df_f = df_f.where(pd.notnull(df_f), None)
    print(len(df_f))
    df_f.head(3)
    df_f ['Overall stage'] = df_f[['M','T','N','all','AJCC']].apply(extract_staging,axis=1)
    set(df_f['all'].values)
    
    #Deleting duplicates
    mask_duplicates = df_f.duplicated(subset='MRN', keep='first')
    df_f = df_f[~mask_duplicates]
    
    #Change columns order and sorting
    df_f = df_f.reindex(columns=['MRN','MATCH TYPE','Cancer Type','BIRTHDATE','T','N','M','all','ER','PR','Her2','AJCC','Overall stage'])
    df_f = df_f.sort_values(by='MRN')
    
    df_f.to_excel('result_v1.xlsx')
    
    pks = dict()
    sqlite_cursor = sqlite.cursor()

    print("processing results into sqlite")
    index = 1
    if df_f is not None:
        for _, row in df_f.iterrows():
            #sqlite_cursor.execute(f"insert into patient (pat_id, mrn, dob, cancer_type, new_or_progressed, date_screened) values ('{pat_id}','{mrn}','{dob}','Breast','{match_type}','{datetime.now()}')")
            sqlite_cursor.execute("insert into patient (pat_id, mrn, dob, cancer_type, new_or_progressed, date_screened) values ('%(pat_id)s','%(mrn)s','%(dob)s','%(disease)s','%(match_type)s','%(now)s')" % {'pat_id': index, 'mrn': row['MRN'], 'dob': '1/1/1975', 'disease': row['Cancer Type'],'match_type': row['MATCH TYPE'], 'now': datetime.now()})
            #pks[ind] = sqlite_cursor.lastrowid
            print(f"pat_id: {index} , mrn: {row['MRN']} , dob: {row['BIRTHDATE']}, disease: {row['Cancer Type']} ,match_type: {row['MATCH TYPE']}, now: {datetime.now()} ")
            sqlite_cursor.execute ("insert into patient_staging (fk_id, stage, stage_t, stage_n, stage_m) values (%(pat_id)s,'%(stage)s','%(T)s','%(N)s','%(M)s')" % {'pat_id': index, 'stage': row['Overall stage'], 'T': row['T'], 'N': row['N'], 'M': row['M']})
            print(f"pat_id: {index}, 'stage': {row['Overall stage']} , 'T': {row['T']} , 'N': {row['N']} , 'M': {row['M']} ")
            sqlite_cursor.execute ("insert into patient_receptor values ('%(pat_id)s','%(key)s','%(value)s')" % {'pat_id': index, 'key': 'ER', 'value': row['ER']})
            print(f"pat_id: {index}, key: ER, value: {row['ER']}")
            sqlite_cursor.execute ("insert into patient_receptor values ('%(pat_id)s','%(key)s','%(value)s')" % {'pat_id': index, 'key': 'PR', 'value': row['PR']}) 
            print(f"pat_id: {index}, key: PR, value: {row['PR']}")
            sqlite_cursor.execute ("insert into patient_receptor values ('%(pat_id)s','%(key)s','%(value)s')" % {'pat_id': index, 'key': 'HER2', 'value': row['Her2']})
            print(f"pat_id: {index}, key: HER2, value: {row['Her2']}")
            index+=1    

    sqlite.commit()
    sqlite.close()
    today = datetime.today()
    print(today.strftime("%Y/%m/%d %H:%M:%S"))

