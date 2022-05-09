"""
matcher.py

VERION: 0.1

INPUTS
======
- cancer patients in SEC
- cancer trials in SEC

OUTPUTS
=======
- matched patient and trial list

STEPS
=====

1. Query the datastores
2. Print out the results



"""

import sqlite3
import csv
import os
from datetime import datetime

sqlite = sqlite3.connect('disrupt.db')

sqlite_cursor = sqlite.cursor()


#NOTE: currently the drug join is left because we found no matches there

#TO-DO: add filter on disease subquery's date of screening in the overall where clause (otherwise we return everything every time)

sql = """select disease.nci_number,date_screened, disease.mrn, cancer_type, stage_match,receptor_match,treatment_match
 from
 (
  select nci_number, mrn,a.pk_id as pt_pk, c.pk_id as trial_pk, a.cancer_type,date_screened
 from
 patient a left join
 trial_cancer_type b on a.cancer_type = b.cancer_type left join
 trial c on b.fk_id = c.pk_id
 ) disease
 inner join
 (
 select nci_number, mrn,a.pk_id as pt_pk, d.pk_id as trial_pk, group_concat(distinct c.stage) as stage_match, count(distinct c.stage) as num_matches
 from
 patient a left join
 patient_staging b on a.pk_id = b.fk_id left join
 trial_stage c on  b.stage = c.stage left join
 trial d on c.fk_id = d.pk_id
 group by  nci_number, mrn,a.pk_id, d.pk_id
 ) stage on disease.pt_pk = stage.pt_pk and disease.trial_pk = stage.trial_pk
 inner join
 (
   select nci_number, mrn,a.pk_id as pt_pk, d.pk_id as trial_pk, group_concat(distinct c.receptor_value) as receptor_match, count(distinct c.receptor_type) as num_matches
 from
 patient a left join
 patient_receptor b on a.pk_id = b.fk_id left join
 trial_receptor c on  b.receptor_type = c.receptor_type and b.receptor_value = c.receptor_value left join
 trial d on c.fk_id = d.pk_id
 group by  nci_number, mrn,a.pk_id, d.pk_id
 ) receptor on disease.pt_pk = receptor.pt_pk and disease.trial_pk = receptor.trial_pk
 left join
 (
   select nci_number, mrn,a.pk_id as pt_pk, d.pk_id as trial_pk, group_concat(distinct c.treatment_name) as treatment_match, count(distinct c.treatment_name) as num_matches
 from
 patient a left join
 patient_treatment b on a.pk_id = b.fk_id left join
 trial_treatment c on  b.treatment_name = c.treatment_name and b.treatment_type = c.treatment_type left join
 trial d on c.fk_id = d.pk_id
 group by  nci_number, mrn,a.pk_id, d.pk_id
 ) tx on disease.pt_pk = tx.pt_pk and disease.trial_pk = tx.trial_pk"""

cursor = sqlite.cursor()

cursor.execute(sql)
results = cursor.fetchall()
today = datetime.today()
print(today.strftime("%Y/%m/%d %H:%M:%S"))

outfile = open("matches/matches_" + today.strftime("%Y-%m-%d") + ".txt", 'w')
writer =csv.writer(outfile)

writer.writerow(['NCI_NUMBER','DATE_SCREENED','MRN','CANCER_TYPE','STAGE_MATCH','RECEPTOR_MATCH','TREATMENT_MATCH'])

for nci_number,date_screened,mrn,cancer_type,stage_match,receptor_match,treatment_match in results:
    writer.writerow([nci_number,date_screened,mrn,cancer_type,stage_match,receptor_match,treatment_match])

outfile.close()
