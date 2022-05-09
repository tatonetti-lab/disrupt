import pyodbc
from getpass import getpass
import jaydebeapi
import sys
from datetime import datetime
import re
import itertools
import sqlite3
from collections import defaultdict


breast_t = ['TX','T0','Tis','T1','T2','T3','T4']
breast_n = ['NX','N0','N1mi','N1a','N1b','N1','N2a','N2b','N2','N3a','N3b','N3c','N3']
breast_m = ['M0(i+)','M0','M1']

driver_name = "net.sourceforge.jtds.jdbc.Driver"

# jTDS Connection string.
connection_url = "jdbc:jtds:sqlserver://DRTUAT01JUP01.sis.nyp.org:1433/clarity;useLOBs=false"

password = getpass('Please enter NYP password for Jupiter: ')

# jTDS Connection properties.
# Some additional connection properties you may want to use
# "domain": "<domain>"
# "ssl": "require"
# "useNTLMv2": "true"
# See the FAQ for details http://jtds.sourceforge.net/faq.html
connection_properties = {
"user": "bem7004",
    "password": password,
    "DOMAIN":"nyh"

}

# Path to jTDS Jar
jar_path = "/home/blm14/.dbvis//config130/databaseinfo/user/driverTypes/sqlserver_43898565-6ce7-4ef7-917c-ba072fb92a14/maven/net/sourceforge/jtds/jtds/1.3.1/jtds-1.3.1.jar"

# Establish connection.
connection = jaydebeapi.connect(driver_name, connection_url, connection_properties, jar_path)
cursor = connection.cursor()

sql = "select * from cdndb.bem7004.notetext"
print(' starting pull of notes matching pattern from temp table')
cursor.execute(sql)
results = cursor.fetchall()
count=0
newpts = list()
staging_matches = defaultdict(lambda: defaultdict(set))
for pat_id,note_id, date_of_service,match_type,note_text in results:
    # re.search(regex,string)
    #print(str(note_text))
    
    count=count+1
    #print("count " + str(count))
    m = re.findall('[a-zA-Z0-9]*([Tt][Xx0-4]([a-dA-D]|is|IS)*) *(p|c)*([Nn][Xx0-3][a-dA-D]{0,1})* *(p|c)*([Mm][0-1xX])*([a-zA-Z0-9]*)',str(note_text))
    
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
            print(match)
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
        
    if m is not None:
        found = True
        for match in m:
            print(match)
            print(note_id)
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
    if m is not None:
        print("match")
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

for (pat_id, note_id),matches in staging_matches.items():
    print(f"note_id: {note_id}")
    print(f"pat_id: {pat_id}")
    print(f"matches: {matches}")
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
            })
for pt in newpts:
    print(pt)

