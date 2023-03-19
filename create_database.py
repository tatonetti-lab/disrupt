import sqlite3

con = sqlite3.connect('disrupt.db')

con.execute(''' drop table if exists patient ''')

con.execute(''' drop table if exists patient_staging ''')

con.execute(''' drop table if exists patient_receptor ''')

con.execute(''' drop table if exists patient_treatment ''')

con.execute(''' drop table if exists trial ''')

con.execute(''' drop table if exists trial_cancer_type ''')

con.execute(''' drop table if exists trial_stage ''')

con.execute(''' drop table if exists trial_receptor ''')

con.execute(''' drop table if exists trial_treatment ''')

con.execute(''' create table patient (pk_id integer primary key, pat_id text, mrn text, dob text, cancer_type text,  new_or_progressed text, date_screened text) ''')

con.execute(''' create table patient_staging (fk_id integer, stage text, stage_t text, stage_n text, stage_m text) ''')

con.execute(''' create table patient_receptor (fk_id integer, receptor_type text, receptor_value text ) ''')

con.execute(''' create table patient_treatment (fk_id integer, treatment_type text, treatment_name text, treatment_start_date text, treatment_end_date text) ''')

con.execute(''' create table trial (pk_id integer primary key, nci_number text, date_parsed text) ''')

con.execute(''' create table trial_cancer_type (fk_id integer, cancer_type text) ''')

con.execute(''' create table trial_stage (fk_id integer, stage text) ''')

con.execute(''' create table trial_receptor (fk_id integer, receptor_type text, receptor_value text ) ''')

con.execute(''' create table trial_treatment (fk_id integer, treatment_type text, treatment_name text ) ''')

con.execute(''' create table staging_rank (stage_name varchar(100), ranking integer) ''')
 
con.execute(''' insert into staging_rank values  ('Stage 0',1) ''')
con.execute(''' insert into staging_rank values  ('Stage I',2) ''')
con.execute(''' insert into staging_rank values  ('Stage IA',3) ''')
con.execute(''' insert into staging_rank values  ('Stage IB',4) ''')
con.execute(''' insert into staging_rank values  ('Stage II',5) ''')
con.execute(''' insert into staging_rank values  ('Stage IIA',6) ''')
con.execute(''' insert into staging_rank values  ('Stage IIB',7) ''')
con.execute(''' insert into staging_rank values  ('Stage III',8) ''')
con.execute(''' insert into staging_rank values  ('Stage IIIA',9) ''')
con.execute(''' insert into staging_rank values  ('Stage IIIB',10) ''')
con.execute(''' insert into staging_rank values  ('Stage IIIC',11) ''')
con.execute(''' insert into staging_rank values  ('Stage IV',12) ''')

con.commit()
