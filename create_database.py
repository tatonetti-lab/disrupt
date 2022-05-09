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
