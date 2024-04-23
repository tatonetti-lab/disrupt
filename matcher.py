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
import argparse

parser= argparse.ArgumentParser()

parser.add_argument("--disease",type=str, required=True,help="Please specify a diseae site (breast/liver/prostate/bladder)")

args = parser.parse_args()

disease = args.disease

sqlite = sqlite3.connect('disrupt.db')

sqlite_cursor = sqlite.cursor()

today = datetime.today().strftime("%Y-%m-%d")


#NOTE: currently the drug join is left because we found no matches there

#TO-DO: add filter on disease subquery's date of screening in the overall where clause (otherwise we return everything every time) DONE

#TO-DO: ensure ALL receptor statuses match.

if(disease == 'Breast' or disease == 'breast'):

    sql = """select distinct * from (select disease.nci_number,date_screened, new_or_progressed,disease.mrn, cancer_type, stage_match,receptor_match,treatment_match
     from
     (
      select nci_number, mrn,a.pk_id as pt_pk, c.pk_id as trial_pk, a.cancer_type,date_screened, new_or_progressed
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
     trial d on c.fk_id = d.pk_id left join
     staging_rank e on b.stage = e.stage_name
     where ranking = (select max(ranking) from staging_rank q inner join patient_staging qq on q.stage_name = qq.stage where qq.fk_id = b.fk_id)
     group by  nci_number, mrn,a.pk_id, d.pk_id
     ) stage on disease.pt_pk = stage.pt_pk and disease.trial_pk = stage.trial_pk
     inner join
     (
    select * from
     (
     select nci_number, mrn, trial_pk, pt_pk,group_concat(distinct case when patient_receptor = trial_receptor and cnt_types_for_this_trial = 1 then patient_receptor else null end) as receptor_match,
     sum(case when receptor_type = 'PR' and (cnt_types_for_this_trial in (0,2) or (trial_receptor = patient_receptor)) then 1 else 0 end) as pr_match,
     sum(case when receptor_type = 'ER' and (cnt_types_for_this_trial in (0,2) or (trial_receptor = patient_receptor)) then 1 else 0 end) as er_match,
     sum(case when receptor_type = 'HER2' and (cnt_types_for_this_trial in (0,2) or (trial_receptor = patient_receptor)) then 1 else 0 end) as her2_match,
     sum(case when receptor_type = 'PR' then 1 else 0 end) as pr_req,
     sum(case when receptor_type = 'ER' then 1 else 0 end) as er_req,
     sum(case when receptor_type = 'HER2' then 1 else 0 end) as her2_req
     from
     (
        select nci_number, mrn,a.pk_id as pt_pk, d.pk_id as trial_pk, c.receptor_type, c.receptor_value,
        (select count(q.receptor_value) from trial_receptor q where c.receptor_type = q.receptor_type and d.pk_id = q.fk_id) as cnt_types_for_this_trial, b.receptor_value as patient_receptor,
        c.receptor_value as trial_receptor
     from
     patient a left join
     patient_receptor b on a.pk_id = b.fk_id left join
     trial_receptor c on  b.receptor_type = c.receptor_type and b.receptor_value = c.receptor_value left join
     trial d on c.fk_id = d.pk_id
    ) group by nci_number, mrn, trial_pk,pt_pk
    ) q where (pr_req = 0 or pr_match>0) and (er_req = 0 or er_match>0) and (her2_req=0 or her2_match>0)
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
     ) tx on disease.pt_pk = tx.pt_pk and disease.trial_pk = tx.trial_pk
     where disease.date_screened >= '%s'
    union all
    select disease.nci_number,date_screened, new_or_progressed, disease.mrn, cancer_type, 'ALL STAGES ALLOWED' as stage_match,receptor_match, treatment_match as treatment_match
     from
     (
      select nci_number, mrn,a.pk_id as pt_pk, c.pk_id as trial_pk, a.cancer_type,date_screened, new_or_progressed
     from
     patient a left join
     trial_cancer_type b on a.cancer_type = b.cancer_type left join
     trial c on b.fk_id = c.pk_id
     ) disease
     inner join
     (
    select * from
     (
     select nci_number, mrn, trial_pk, pt_pk,group_concat(distinct case when patient_receptor = trial_receptor and cnt_types_for_this_trial=1 then patient_receptor else null end) as receptor_match,
     sum(case when receptor_type = 'PR' and (cnt_types_for_this_trial in (0,2) or (trial_receptor = patient_receptor)) then 1 else 0 end) as pr_match,
     sum(case when receptor_type = 'ER' and (cnt_types_for_this_trial in (0,2) or (trial_receptor = patient_receptor)) then 1 else 0 end) as er_match,
     sum(case when receptor_type = 'HER2' and (cnt_types_for_this_trial in (0,2) or (trial_receptor = patient_receptor)) then 1 else 0 end) as her2_match,
     sum(case when receptor_type = 'PR' then 1 else 0 end) as pr_req,
     sum(case when receptor_type = 'ER' then 1 else 0 end) as er_req,
     sum(case when receptor_type = 'HER2' then 1 else 0 end) as her2_req
     from
     (
        select nci_number, mrn,a.pk_id as pt_pk, d.pk_id as trial_pk, c.receptor_type, c.receptor_value,
        (select count(q.receptor_value) from trial_receptor q where c.receptor_type = q.receptor_type and d.pk_id = q.fk_id) as cnt_types_for_this_trial, b.receptor_value as patient_receptor,
        c.receptor_value as trial_receptor
     from
     patient a left join
     patient_receptor b on a.pk_id = b.fk_id left join
     trial_receptor c on  b.receptor_type = c.receptor_type and b.receptor_value = c.receptor_value left join
     trial d on c.fk_id = d.pk_id
    ) group by nci_number, mrn, trial_pk,pt_pk
    ) q where (pr_req = 0 or pr_match>0) and (er_req = 0 or er_match>0) and (her2_req=0 or her2_match>0)
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
     ) tx on disease.pt_pk = tx.pt_pk and disease.trial_pk = tx.trial_pk
     where not exists (select 1 from trial_stage q where disease.trial_pk = q.fk_id)
     and disease.date_screened >= '%s'
     union all
     select disease.nci_number,date_screened, new_or_progressed,disease.mrn, cancer_type, stage_match,'ALL RECEPTORS ALLOWED' as receptor_match,treatment_match
     from
     (
      select nci_number, mrn,a.pk_id as pt_pk, c.pk_id as trial_pk, a.cancer_type,date_screened,new_or_progressed
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
     trial d on c.fk_id = d.pk_id left join
     staging_rank e on b.stage = e.stage_name
     where ranking = (select max(ranking) from staging_rank q inner join patient_staging qq on q.stage_name = qq.stage where qq.fk_id = b.fk_id)
     group by  nci_number, mrn,a.pk_id, d.pk_id
     ) stage on disease.pt_pk = stage.pt_pk and disease.trial_pk = stage.trial_pk
     left join
     (
       select nci_number, mrn,a.pk_id as pt_pk, d.pk_id as trial_pk, group_concat(distinct c.treatment_name) as treatment_match, count(distinct c.treatment_name) as num_matches
     from
     patient a left join
     patient_treatment b on a.pk_id = b.fk_id left join
     trial_treatment c on  b.treatment_name = c.treatment_name and b.treatment_type = c.treatment_type left join
     trial d on c.fk_id = d.pk_id
     group by  nci_number, mrn,a.pk_id, d.pk_id
     ) tx on disease.pt_pk = tx.pt_pk and disease.trial_pk = tx.trial_pk
      where not exists (select 1 from trial_receptor q where disease.trial_pk = q.fk_id) and  disease.date_screened > '%s'
    union all
    select disease.nci_number,date_screened, new_or_progressed,disease.mrn, cancer_type, 'ALL STAGES ALLOWED' as staging_match,'ALL RECEPTORS ALLOWED' as receptor_match,treatment_match
     from
     (
      select nci_number, mrn,a.pk_id as pt_pk, c.pk_id as trial_pk, a.cancer_type,date_screened,new_or_progressed
     from
     patient a left join
     trial_cancer_type b on a.cancer_type = b.cancer_type left join
     trial c on b.fk_id = c.pk_id
     ) disease
     left join
     (
       select nci_number, mrn,a.pk_id as pt_pk, d.pk_id as trial_pk, group_concat(distinct c.treatment_name) as treatment_match, count(distinct c.treatment_name) as num_matches
     from
     patient a left join
     patient_treatment b on a.pk_id = b.fk_id left join
     trial_treatment c on  b.treatment_name = c.treatment_name and b.treatment_type = c.treatment_type left join
     trial d on c.fk_id = d.pk_id
     group by  nci_number, mrn,a.pk_id, d.pk_id
     ) tx on disease.pt_pk = tx.pt_pk and disease.trial_pk = tx.trial_pk 
     where not exists (select 1 from trial_receptor q where disease.trial_pk = q.fk_id)
    and not exists (select 1 from trial_stage q where disease.trial_pk = q.fk_id) and disease.date_screened >= '%s' ) q""" % (today,today,today,today)

elif (disease == 'Lung' or disease == 'lung'):
    sql = """select disease.nci_number,date_screened, new_or_progressed,disease.mrn, cancer_type, stage_match,receptor_match,treatment_match,
group_concat(distinct hugo_symbol) as matched_genes
     from
     (
      select nci_number, mrn,a.pk_id as pt_pk, c.pk_id as trial_pk, a.cancer_type,date_screened, new_or_progressed
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
     trial d on c.fk_id = d.pk_id left join
     staging_rank e on b.stage = e.stage_name
     where ranking = (select max(ranking) from staging_rank q inner join patient_staging qq on q.stage_name = qq.stage where qq.fk_id = b.fk_id)
     group by  nci_number, mrn,a.pk_id, d.pk_id
     ) stage on disease.pt_pk = stage.pt_pk and disease.trial_pk = stage.trial_pk
     left join
     (
    select * from
     (
     select nci_number, mrn, trial_pk, pt_pk,group_concat(distinct case when patient_receptor = trial_receptor and cnt_types_for_this_trial = 1 then patient_receptor else null end) as receptor_match,
     sum(case when receptor_type = 'PD_L1' and (cnt_types_for_this_trial in (0,2) or (trial_receptor = patient_receptor)) then 1 else 0 end) as pd_l1_match,
     sum(case when receptor_type = 'PD_L1' then 1 else 0 end) as pd_l1_req
     from
     (
        select nci_number, mrn,a.pk_id as pt_pk, d.pk_id as trial_pk, c.receptor_type, c.receptor_value,
        (select count(q.receptor_value) from trial_receptor q where c.receptor_type = q.receptor_type and d.pk_id = q.fk_id) as cnt_types_for_this_trial, b.receptor_value as patient_receptor,
        c.receptor_value as trial_receptor
     from
     patient a left join
     patient_receptor b on a.pk_id = b.fk_id left join
     trial_receptor c on  b.receptor_type = c.receptor_type and b.receptor_value = c.receptor_value left join
     trial d on c.fk_id = d.pk_id
    ) group by nci_number, mrn, trial_pk,pt_pk
    ) q where (pd_l1_req = 0 or pd_l1_match>0) 
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
     ) tx on disease.pt_pk = tx.pt_pk and disease.trial_pk = tx.trial_pk left join
     (
     select pk_id as pt_pk, hugo_gene, hugo_symbol, d.fk_id as trial_pk
    from
    patient a left join
    patient_genes b on pk_id = b.fk_id left join
    genes c on replace(case when hugo_gene like '%(%' then substring(hugo_gene,1,instr(hugo_gene,'(')-1)
    when hugo_gene like '%:%' then substring(hugo_gene,1,instr(hugo_gene,':')-1)
     else hugo_gene end,': amplification','') = hugo_symbol left join
    trial_receptor d on receptor_type = 'Gene' and receptor_value like '%' || hugo_symbol || '%'
    ) genes on disease.pt_pk = genes.pt_pk and disease.trial_pk = genes.trial_pk
     where disease.date_screened >= '2024-04-20'
     group by disease.nci_number,date_screened, new_or_progressed,disease.mrn, cancer_type, stage_match,receptor_match,treatment_match""" 

cursor = sqlite.cursor()

print(sql)

cursor.execute(sql)
results = cursor.fetchall()
today = datetime.today()
print(today.strftime("%Y/%m/%d %H:%M:%S"))

outfile = open("matches/matches_" + today.strftime("%Y-%m-%d") + ".txt", 'w')
writer =csv.writer(outfile)

writer.writerow(['NCI_NUMBER','URL','DATE_SCREENED','NEW_OR_PROGRESSED','MRN','CANCER_TYPE','STAGE_MATCH','RECEPTOR_MATCH','TREATMENT_MATCH','GENES'])

for nci_number,date_screened,new_or_progressed,mrn,cancer_type,stage_match,receptor_match,treatment_match,matched_genes in results:
    url = 'https://www.cancer.gov/about-cancer/treatment/clinical-trials/search/v?' +nci_number
    writer.writerow([nci_number,url,date_screened,new_or_progressed,mrn,cancer_type,stage_match,receptor_match,treatment_match,matched_genes])

outfile.close()
