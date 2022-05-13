"""
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
from getpass import getpass
import jaydebeapi
import sys
from datetime import datetime
import re
import itertools
import sqlite3
from collections import defaultdict
import json
import argparse
import sqlparse
import os

sqlite = sqlite3.connect('disrupt.db')

breast_t = ['TX','T0','Tis','T1','T2','T3','T4']
breast_n = ['NX','N0','N1mi','N1a','N1b','N1','N2a','N2b','N2','N3a','N3b','N3c','N3']
breast_m = ['M0(i+)','M0','M1']

prostate_t = ['T1a','T1b','T1c','T1','T2a','T2b','T2c','T2','T3a','T3b','T3','T4']
prostate_n = ['NX','N0','N1']
prostate_m = ['MX','M0','M1a','M1b','M1c','M1d','M1']

liver_t = ['T0','T1a','T1b','T1','T2','T3','T4','TX']
liver_n = ['N0','N1','NX']
liver_m = ['M0','M1']


def breast_note_parse(results):
        staging_matches = defaultdict(lambda: defaultdict(set))
        for pat_id,note_id, date_of_service,match_type,note_text in results:

                count=count+1
                m = re.findall('[a-zA-Z0-9]*([Tt][Xx0-4]([a-dA-D]|is|IS)*) *(p|c)*([Nn][Xx0-3][a-dA-D]{0,1})* *(p|c)*([Mm][0-1xX])*([a-zA-Z0-9]*)',str(note_text))
                #print("match type 1 - non-pretty staging")
                loose_t = ""
                loose_n = ""
                loose_m = ""
                er = ""
                pr = ""
                her2 = ""
                overallstage = ""

                found = False

                if m is not None:
                        for match in m:
                                #print(match)
                                loose_t = match[0]
                                loose_n = match[3]
                                loose_m = match[5]
                                if loose_t != '':
                                        staging_matches[(pat_id,note_id)]['T'].add(loose_t)

                                if loose_n != '':
                                        staging_matches[(pat_id,note_id)]['N'].add(loose_n)

                                if loose_m != '':
                                        staging_matches[(pat_id, note_id)]['M'].add(loose_m)


                                        found = True
                m = re.findall('([/ \(](HER2|ER|PR)[ \-+]([PpNn]\w*)*)',str(note_text))
                #print("match type 2 - non-pretty receptors")
                if m is not None:
                        found = True
                        for match in m:
                                #print(match)
                                #print(note_id)
                                if match[1] == 'HER2' and not 'Neu' in match[0]:
                                        if 'neg' in match[0].lower():
                                                her2 = 'HER2-'
                                        elif 'pos' in match[0].lower():
                                                her2 = 'HER2+'
                                        elif '+' in match[0].lower():
                                                her2 = 'HER2+'
                                        elif '-' in match[0].lower():
                                                her2 = 'HER2-'
                                elif match[1] == 'ER':
                                        if 'neg' in match[0].lower():
                                                er = 'ER-'
                                        elif 'pos' in match[0].lower():
                                                er = 'ER+'
                                        elif '+' in match[0].lower():
                                                er = 'ER+'
                                        elif '-' in match[0].lower():
                                                er = 'ER-'

                                elif match[1] == 'PR':
                                        if 'neg' in match[0].lower():
                                                pr = 'PR-'
                                        elif 'pos' in match[0].lower():
                                                pr = 'PR+'
                                        elif '+' in match[0].lower():
                                                pr = 'PR+'
                                        elif '-' in match[0].lower():
                                                pr = 'PR-'
                                if(her2 != ''):
                                        staging_matches[(pat_id, note_id)]['HER2'].add(her2)
                                if(er != ''):
                                        staging_matches[(pat_id, note_id)]['ER'].add(er)
                                if(pr != ''):
                                        staging_matches[(pat_id, note_id)]['PR'].add(pr)



                m=re.search('.*AJCC ([0-9]{1,2}[a-z][a-z]) Edition.+?(Clinical|Pathologic): (Stage \w+ \(.*?\) )',str(note_text))
                #print("match type 3 - pretty staging + receptor status")
                if m is not None:
                        ajcc = m.group(1)
                        path_or_clin = m.group(2)
                        staging = m.group(3)
                        openparen = staging.index('(')
                        closeparen = staging.rfind(')')
                        overallstage = staging[0:(openparen-1)];
                        staging_str = staging[(openparen+1):closeparen]

                        t_match = ""
                        n_match = ""
                        m_match = ""
                        er_match = ""
                        pr_match = ""
                        her2_match = ""
                        oncotype_match = ""
                        g_match = ""

                        for item_u in staging_str.split(","):
                                item=item_u.strip(" ")
                                #print(item.strip(" "))
                                found = False
                                for matchterm in breast_t:
                                        if item.find(matchterm) != -1:
                                                t_match=item
                                                found = True
                                for matchterm in breast_n:
                                        if item.find(matchterm) != -1:
                                                n_match = item
                                                found = True
                                for matchterm in breast_m:
                                        if item.find(matchterm) != -1:
                                                m_match = item
                                                found=True
                                if found == False:
                                        if item.find('HER2') != -1:
                                                her2_match = item
                                                found = True
                                        elif item.find("Onco") != -1:
                                                oncotype_match = item
                                                found = True
                                        elif item.find("ER") != -1:
                                                er_match = item
                                                found = True
                                        elif item.find("PR") != -1:
                                                pr_match = item
                                                found = True
                                        elif item.find("G") != -1:
                                                g_match = item
                                                found = True
                                if overallstage != '':
                                        staging_matches[(pat_id, note_id)]['STAGE'].add(overallstage)
                                if t_match != '':
                                        staging_matches[(pat_id, note_id)]['T'].add(t_match)
                                if n_match != '':
                                        staging_matches[(pat_id, note_id)]['N'].add(n_match)
                                if m_match != '':
                                        staging_matches[(pat_id, note_id)]['M'].add(m_match)
                                if her2_match != '':
                                        staging_matches[(pat_id, note_id)]['HER2'].add(her2_match)
                                if er_match != '':
                                        staging_matches[(pat_id, note_id)]['ER'].add(er_match)
                                if pr_match != '':
                                        staging_matches[(pat_id, note_id)]['PR'].add(pr_match)
                                if g_match != '':
                                        staging_matches[(pat_id, note_id)]['G'].add(g_match)
                                if oncotype_match != '':
                                        staging_matches[(pat_id, note_id)]['ONCO'].add(oncotype_match)
        return staging_matches

def breast_process(staging_matches):
        newpts = list()
        for (pat_id, note_id),matches in staging_matches.items():
                for k in ('T','N','M','HER2','ER','PR','G','ONCO','STAGE'):
                        if len(matches[k]) == 0:
                                matches[k] = set([""])
                                match_iterator = itertools.product(*[matches[k] for k in ('T','N','M','HER2','ER','PR','G','ONCO','STAGE')])
                for (T,N,M,HER2,ER,PR,G,ONCO,STAGE) in match_iterator:
                        if(STAGE == ""):
                                if 'm1' in M.lower():
                                        overallstage = 'Stage IV'
                                elif 'tis' in T.lower() and 'n0' in N.lower():
                                        overallstage = 'Stage 0'
                                elif 't1' in T.lower() and 'n0' in N.lower():
                                        overallstage = 'Stage 1A'
                                elif 't0' in T.lower() and 'n1mi' in N.lower():
                                        overallstage = 'Stage 1B'
                                elif 't1' in T.lower() and 'n1mi' in N.lower():
                                        overallstage = 'Stage 1B'
                                elif 't0' in T.lower() and 'n1' in N.lower():
                                        overallstage = 'Stage IIA'
                                elif 't1' in T.lower() and 'n1' in N.lower():
                                        overallstage = 'Stage IIA'
                                elif 't2' in T.lower() and 'n0' in N.lower():
                                        overallstage = 'Stage IIA'
                                elif 't2' in T.lower() and 'n1' in N.lower():
                                        overallstage = 'Stage IIB'
                                elif 't3' in T.lower() and 'n0' in N.lower():
                                        overallstage = 'Stage IIB'
                                elif 't0' in T.lower() and 'n2' in N.lower():
                                        overallstage = 'Stage IIIA'
                                elif 't1' in T.lower() and 'n2' in N.lower():
                                        overallstage = 'Stage IIIA'
                                elif 't2' in T.lower() and 'n2' in N.lower():
                                        overallstage = 'Stage IIIA'
                                elif 't3' in T.lower() and 'n1' in N.lower():
                                        overallstage = 'Stage IIIA'
                                elif 't3' in T.lower() and 'n2' in N.lower():
                                        overallstage = 'Stage IIIA'
                                elif 't4' in T.lower() and 'n0' in N.lower():
                                        overallstage = 'Stage IIIB'
                                elif 't4' in T.lower() and 'n1' in N.lower():
                                        overallstage = 'Stage IIIB'
                                elif 't4' in T.lower() and 'n2' in N.lower():
                                        overallstage = 'Stage IIIB'
                                elif 'n3' in N.lower():
                                        overallstage = 'Stage IIIC'
                                else:
                                        overallstage = "NOT ENOUGH INFORMATION"
                                STAGE = overallstage
                        #print(f"note_id: {note_id}")
                        #print(f"pat_id: {pat_id}")
                        #print(f"stage: {STAGE}")
                        #print(f"matches: {matches}")

                        newpts.append(
                                {
                                        "PAT_ID":pat_id,
                                        "NOTE_ID":note_id,
                                        "T":T,
                                        "N":N,
                                        "M":M,
                                        "HER2":HER2,
                                        "ER":ER,
                                        "PR":PR,
                                        "G":G,
                                        "ONCO":ONCO,
                                        "STAGE":STAGE
                                }
                        )
        return newpts
        
def liver_note_parse(note):
        staging_matches = defaultdict(lambda: defaultdict(set))
        for pat_id,note_id, date_of_service,match_type,note_text in results:

                count=count+1
                m = re.findall('[a-zA-Z0-9]*([Tt][Xx0-4]([a-dA-D]|is|IS)*) *(p|c)*([Nn][Xx0-3][a-dA-D]{0,1})* *(p|c)*([Mm][0-1xX])*([a-zA-Z0-9]*)',str(note_text))
                #print("match type 1 - non-pretty staging")
                loose_t = ""
                loose_n = ""
                loose_m = ""
                er = ""
                pr = ""
                her2 = ""
                overallstage = ""

                found = False

                if m is not None:
                        for match in m:
                                #print(match)
                                loose_t = match[0]
                                loose_n = match[3]
                                loose_m = match[5]
                                if loose_t != '':
                                        staging_matches[(pat_id,note_id)]['T'].add(loose_t)

                                if loose_n != '':
                                        staging_matches[(pat_id,note_id)]['N'].add(loose_n)

                                if loose_m != '':
                                        staging_matches[(pat_id, note_id)]['M'].add(loose_m)


                                        found = True

                m=re.search('.*AJCC ([0-9]{1,2}[a-z][a-z]) Edition.+?(Clinical|Pathologic): (Stage \w+ \(.*?\) )',str(note_text))
                #print("match type 3 - pretty staging + receptor status")
                if m is not None:
                        ajcc = m.group(1)
                        path_or_clin = m.group(2)
                        staging = m.group(3)
                        openparen = staging.index('(')
                        closeparen = staging.rfind(')')
                        overallstage = staging[0:(openparen-1)];
                        staging_str = staging[(openparen+1):closeparen]

                        t_match = ""
                        n_match = ""
                        m_match = ""
                        er_match = ""
                        pr_match = ""
                        her2_match = ""
                        oncotype_match = ""
                        g_match = ""

                        for item_u in staging_str.split(","):
                                item=item_u.strip(" ")
                                #print(item.strip(" "))
                                found = False
                                for matchterm in breast_t:
                                        if item.find(matchterm) != -1:
                                                t_match=item
                                                found = True
                                for matchterm in breast_n:
                                        if item.find(matchterm) != -1:
                                                n_match = item
                                                found = True
                                for matchterm in breast_m:
                                        if item.find(matchterm) != -1:
                                                m_match = item
                                                found=True
                                if found == False:
                                        if item.find('HER2') != -1:
                                                her2_match = item
                                                found = True
                                        elif item.find("Onco") != -1:
                                                oncotype_match = item
                                                found = True
                                        elif item.find("ER") != -1:
                                                er_match = item
                                                found = True
                                        elif item.find("PR") != -1:
                                                pr_match = item
                                                found = True
                                        elif item.find("G") != -1:
                                                g_match = item
                                                found = True
                                if overallstage != '':
                                        staging_matches[(pat_id, note_id)]['STAGE'].add(overallstage)
                                if t_match != '':
                                        staging_matches[(pat_id, note_id)]['T'].add(t_match)
                                if n_match != '':
                                        staging_matches[(pat_id, note_id)]['N'].add(n_match)
                                if m_match != '':
                                        staging_matches[(pat_id, note_id)]['M'].add(m_match)
                                if her2_match != '':
                                        staging_matches[(pat_id, note_id)]['HER2'].add(her2_match)
                                if er_match != '':
                                        staging_matches[(pat_id, note_id)]['ER'].add(er_match)
                                if pr_match != '':
                                        staging_matches[(pat_id, note_id)]['PR'].add(pr_match)
                                if g_match != '':
                                        staging_matches[(pat_id, note_id)]['G'].add(g_match)
                                if oncotype_match != '':
                                        staging_matches[(pat_id, note_id)]['ONCO'].add(oncotype_match)
        return staging_matches
        
def prostate_note_parse(note):
        pass
        

def main():
        parser= argparse.ArgumentParser()
        parser.add_argument("--config",type=str,required=True,help="Please specify a path to the config file")
        parser.add_argument("--disease",type=str, required=True,help="Please specify a diseae site (breast/liver/prostate)")
        args = parser.parse_args()
        config = json.loads(open(args.config).read())

        java_home = os.environ.setdefault("JAVA_HOME",config['JAVA_HOME'])

        disease = args.disease

        #try:
        # jTDS Driver.
        driver_name = config['connectionprops']['jdbc_driver']


        # jTDS Connection string.
        connection_url = config['connectionprops']['jdbc_conn_string']

        password = getpass('Please enter database password: ')

        # jTDS Connection properties.
        # Some additional connection properties you may want to use
        # "domain": "<domain>"
        # "ssl": "require"
        # "useNTLMv2": "true"
        # See the FAQ for details http://jtds.sourceforge.net/faq.html
        connection_properties = {
            "user": config['connectionprops']['user'],
            "password": password,
            "DOMAIN":config['connectionprops']['domain']

        }

        # Path to jTDS Jar
        jar_path = config['connectionprops']['jar_path']

        # Establish connection.
        connection = jaydebeapi.connect(driver_name, connection_url, connection_properties, jar_path)
        cursor = connection.cursor()



        #todo - move this into a single loop of ['diseases']['breast']['queries']
        print("appointments")
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))
        for sql in config['diseases'][disease]['queries']['appointments']:
                print(sqlparse.format(sql,reindent=True,keyword_case='upper' ))
                cursor.execute(sql)
        print("done")
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))

        print("diagnoses")
        for sql in config['diseases'][disease]['queries']['diagnoses']:
                print(sqlparse.format(sql,reindent=True,keyword_case='upper' ))
                cursor.execute(sql)
        print("Done")
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))

        print('creating cohort table, initial patients first')
        for sql in config['diseases'][disease]['queries']['newpts']:
                print(sqlparse.format(sql,reindent=True,keyword_case='upper' ))
                cursor.execute(sql)
        print("Done")
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))

        print("finding progressive disease pts")
        for sql in config['diseases'][disease]['queries']['recurredpts']:
                print(sqlparse.format(sql,reindent=True,keyword_case='upper' ))
                cursor.execute(sql)
        print("Done")
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))

        print("notes")
        for sql in config['diseases'][disease]['queries']['notes']:
                print(sqlparse.format(sql,reindent=True,keyword_case='upper' ))
                cursor.execute(sql)
        print("Done")
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))

        print("meds")
        for sql in config['diseases'][disease]['queries']['meds']:
                print(sqlparse.format(sql,reindent=True,keyword_case='upper' ))
                cursor.execute(sql)
        print("Done")
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))

        sql = config['diseases'][disease]['queries']['final']['notes']
        print(' starting pull of notes matching pattern from temp table')
        cursor.execute(sql)
        results = cursor.fetchall()
        count=0

        #note: add switch statement for disease param
        staging_matches = breast_note_parse(results)
        newpts = breast_process(staging_matches)


        print("done!")
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))

        cursor.execute(config['diseases'][disease]['queries']['final']['meds'])

        results = cursor.fetchall()

        newpt_meds = dict()
        for pat_id, rxnorm_code, drug_name, first_order_date, last_order_date,match_type in results:
                drug_name = drug_name.replace("'","")
                newpt_meds[(pat_id, drug_name)]= {
                        "first_order_date":first_order_date,
                        "last_order_date":last_order_date,
                        "match_type":match_type
                }

        sql = config['diseases'][disease]['queries']['final']['ptmatches']
        cursor.execute(sql)
        results = cursor.fetchall()
        pks = dict()
        sqlite_cursor = sqlite.cursor()
        print("processing results into sqlite")
        for match_type, pat_id, mrn, dob in results:
                #sqlite_cursor.execute(f"insert into patient (pat_id, mrn, dob, cancer_type, new_or_progressed, date_screened) values ('{pat_id}','{mrn}','{dob}','Breast','{match_type}','{datetime.now()}')")
                sqlite_cursor.execute("insert into patient (pat_id, mrn, dob, cancer_type, new_or_progressed, date_screened) values ('%(pat_id)s','%(mrn)s','%(dob)s','%(disease)s','%(match_type)s','%(now)s')" % {'disease':disease,'pat_id': pat_id, 'mrn': mrn, 'dob': dob, 'match_type': match_type, 'now': datetime.now()})
                pks[pat_id] = sqlite_cursor.lastrowid

        for pt in newpts:
                #sqlite_cursor.execute (f"insert into patient_staging (fk_id, stage, stage_t, stage_n, stage_m) values ({pks[pt['PAT_ID']]},'{pt['STAGE']}','{pt['T']}','{pt['N']}','{pt['M']}')")
                sqlite_cursor.execute ("insert into patient_staging (fk_id, stage, stage_t, stage_n, stage_m) values (%(pat_id)s,'%(stage)s','%(T)s','%(N)s','%(M)s')" % {'pat_id': pks[pt['PAT_ID']], 'stage': pt['STAGE'], 'T': pt['T'], 'N': pt['N'], 'M': pt['M']})
                for key in pt.keys():
                        if pt[key] != "":
                                #sqlite_cursor.execute (f"insert into patient_receptor values ('{pks[pt['PAT_ID']]}','HER2','{pt['HER2']}')")
                                sqlite_cursor.execute ("insert into patient_receptor values ('%(pat_id)s','%(key)s','%(value)s')" % {'pat_id': pks[pt['PAT_ID']], 'key':key, 'value': pt[key]})

        for (pat_id, drug_name), drug in newpt_meds.items():
                # sqlite_cursor.execute(f"insert into patient_treatment (fk_id, treatment_type, treatment_name, treatment_start_date, treatment_end_date) values ({pks[pat_id]},'Drug','{drug_name}','{drug['first_order_date']}','{drug['last_order_date']}')")
                sqlite_cursor.execute("insert into patient_treatment (fk_id, treatment_type, treatment_name, treatment_start_date, treatment_end_date) values (%(pat_id)s,'Drug','%(drug_name)s','%(first_order_date)s','%(last_order_date)s')" % {'pat_id': pks[pat_id], 'drug_name': drug_name, 'first_order_date': drug['first_order_date'], 'last_order_date': drug['last_order_date']})


        sqlite.commit()
        sqlite.close()
        today = datetime.today()
        print(today.strftime("%Y/%m/%d %H:%M:%S"))

if __name__ == "__main__":
    sys.exit(main())
