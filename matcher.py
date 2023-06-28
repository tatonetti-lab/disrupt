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

import os
from datetime import datetime
import sqlite3
import csv
import pandas as pd
import numpy as np

sqlite = sqlite3.connect('disrupt.db')

sqlite_cursor = sqlite.cursor()

cursor = sqlite.cursor()

def trialres():
    sql = """SELECT trial.nci_number, trial_cancer_type.cancer_type, trial_stage.stage, trial_receptor.receptor_type, trial_receptor.receptor_value
    FROM trial
    left JOIN trial_cancer_type ON trial.pk_id = trial_cancer_type.fk_id 
    left JOIN trial_stage ON trial.pk_id = trial_stage.fk_id 
    left JOIN trial_receptor ON trial.pk_id = trial_receptor.fk_id"""



    cursor.execute(sql)
    results = cursor.fetchall()
    today = datetime.today()
    print(today.strftime("%Y/%m/%d %H:%M:%S"))


    df = pd.DataFrame({'NCI_NUMBER': pd.Series(dtype='str'),
                       'CANCER_TYPE': pd.Series(dtype='str'),
                       'STAGE': pd.Series(dtype='str'),
                       'RECEPTOR_VALUE': pd.Series(dtype='str')})
    i = 0
    for nci_number,cancer_type,stage,receptor_type,receptor_value in results:
        df.loc[i, ['NCI_NUMBER', 'CANCER_TYPE', 'STAGE', 'RECEPTOR_VALUE']] = nci_number,cancer_type,stage,receptor_value
        i+=1

    df1 = df.replace(np.nan,'',regex=True)
    df1.head(10)


    # group by col1 and aggregate the other columns
    grouped = df1.groupby('NCI_NUMBER').agg({'CANCER_TYPE': lambda x: ','.join(set(x)),
                                      'STAGE': lambda x: ','.join(set(map(str, set(x)))),
                                      'RECEPTOR_VALUE': lambda x: ','.join(set(map(str, set(x))))})

    # reset the index to make col1 a regular column
    df2 = grouped.reset_index()

    # rename the columns
    df2.columns = ['NCI_NUMBER', 'CANCER_TYPE', 'STAGE', 'RECEPTOR_VALUE']


    # Print the final DataFrame
    #df2.to_excel('trialsresult_v1.xlsx')
    


    
    
def patres():
    sql = """SELECT patient.mrn, patient.dob, patient.cancer_type, patient.new_or_progressed, patient.date_screened, patient_staging.stage, patient_receptor.receptor_type, patient_receptor.receptor_value
    FROM patient
    left JOIN patient_staging ON patient.pk_id = patient_staging.fk_id 
    left JOIN patient_receptor ON patient_staging.fk_id = patient_receptor.fk_id"""


    cursor.execute(sql)
    results = cursor.fetchall()
    today = datetime.today()
    print(today.strftime("%Y/%m/%d %H:%M:%S"))


    p_df = pd.DataFrame({'MRN': pd.Series(dtype='str'),
                       'CANCER_TYPE': pd.Series(dtype='str'),
                       'STAGE': pd.Series(dtype='str'),
                       'RECEPTOR_VALUE': pd.Series(dtype='str')})
    i = 0
    for mrn,dob,cancer_type,new_or_progressed,date_screened,stage,receptor_type,receptor_value in results:
        p_df.loc[i, ['MRN', 'CANCER_TYPE', 'STAGE', 'RECEPTOR_VALUE']] = mrn,cancer_type,stage,receptor_value
        i+=1

    p_df1 = p_df.replace(np.nan,'',regex=True)
    p_df1.head(10)


    # group by col1 and aggregate the other columns
    grouped = p_df1.groupby('MRN').agg({'CANCER_TYPE': lambda x: ','.join(set(x)),
                                      'STAGE': lambda x: ','.join(set(map(str, set(x)))),
                                      'RECEPTOR_VALUE': lambda x: ','.join(set(map(str, set(x))))})

    # reset the index to make col1 a regular column
    p_df2 = grouped.reset_index()

    # rename the columns
    p_df2.columns = ['MRN', 'CANCER_TYPE', 'STAGE', 'RECEPTOR_VALUE']

    #p_df2.to_excel('patientsresult_v1.xlsx')


    
def match():
    final_data = []

    for index_p, row_p in p_df2.iterrows():
        p_mrn = row_p['MRN']
        p_cancer_type = row_p['CANCER_TYPE']
        p_stage = row_p['STAGE']
        p_receptor_value = row_p['RECEPTOR_VALUE']
        print(f" Patient  MRN: {p_mrn} CANCER_TYPE: {p_cancer_type} STAGE: {p_stage} RECEPTOR_VALUE: {p_receptor_value}")
        # Extract individual receptor values from p_df2
        p_receptors = p_receptor_value.split(',')
        p_erpos = 'ER+' if 'ER+' in p_receptors else ''
        p_erneg = 'ER-' if 'ER-' in p_receptors else ''
        p_prpos = 'PR+' if 'PR+' in p_receptors else ''
        p_prneg = 'PR-' if 'PR-' in p_receptors else ''
        p_her2pos = 'HER2+' if 'HER2+' in p_receptors else ''
        p_her2neg = 'HER2-' if 'HER2-' in p_receptors else ''
        print(f" Patient  ER+: {p_erpos} ER-: {p_erneg} PR+: {p_prpos} PR-: {p_prneg} Her2+: {p_her2pos} Her2-: {p_her2neg}")
        id = 0
        for index_d, row_d in df2.iterrows():
            nci_number = row_d['NCI_NUMBER']
            df_cancer_type = row_d['CANCER_TYPE']
            df_stage = row_d['STAGE']
            df_receptor_value = row_d['RECEPTOR_VALUE']
            print(f" Trial  NCI_NUMBER: {nci_number} CANCER_TYPE: {df_cancer_type} STAGE: {df_stage} RECEPTOR_VALUE: {df_receptor_value}")
            # Extract individual receptor values from df2
            df_receptors = df_receptor_value.split(',')
            df_erpos = 'ER+' if 'ER+' in df_receptors else ''
            df_erneg = 'ER-' if 'ER-' in df_receptors else ''
            df_prpos = 'PR+' if 'PR+' in df_receptors else ''
            df_prneg = 'PR-' if 'PR-' in df_receptors else ''
            df_her2pos = 'HER2+' if 'HER2+' in df_receptors else ''
            df_her2neg = 'HER2-' if 'HER2-' in df_receptors else ''
            print(f" Trial  ER+: {df_erpos} ER-: {df_erneg} PR+: {df_prpos} PR-: {df_prneg} Her2+: {df_her2pos} Her2-: {df_her2neg}")
            # Check if the cancer type and stage match
            if (p_cancer_type == df_cancer_type or df_cancer_type == ''):
                # Check if the stage matches
                df_stages = df_stage.split(',')
                if 'Stage I' in df_stages:
                    stage1 = 'Stage I'
                    print(stage1)
                if 'Stage II' in df_stages:
                    stage2 = 'Stage II'
                    print(stage2)
                if 'Stage III' in df_stages:
                    stage3 = 'Stage III'
                    print(stage3)
                print(f'Patient stage: {p_stage.strip()} Trial stages: {df_stages}')
                if ((any(p_stage.strip() == stage.strip() for stage in df_stages)) or 
                    (stage1 == 'Stage I' and p_stage.strip() in ['Stage I', 'Stage IA','Stage 1A', 'Stage IB','Stage 1B']) or
                    (stage2 == 'Stage II' and p_stage.strip() in ['Stage II', 'Stage IIA', 'Stage IIB']) or
                    (stage3 == 'Stage III' and p_stage.strip() in ['Stage III', 'Stage IIIA', 'Stage IIIB', 'Stage IIIC'])):
                    print(f"Cancer and Stage matched for {p_mrn} and {nci_number}")
                    # Check if the receptor values match
                    if (((df_erpos == '' and df_erneg == '') or p_erpos.strip() == df_erpos.strip() or p_erneg.strip() == df_erneg.strip()) and
                        ((df_prpos == '' and df_prneg == '') or p_prpos.strip() == df_prpos.strip() or p_prneg.strip() == df_prneg.strip()) and
                        ((df_her2pos == '' and df_her2neg == '') or p_her2pos.strip() == df_her2pos.strip() or p_her2neg.strip() == df_her2neg.strip())):

                        # Create the matched row and append it to the final_data list
                        matched_row = [nci_number, p_mrn, p_cancer_type, p_stage, p_receptor_value]
                        final_data.append(matched_row)
                        id+=1
                        print(f"Matches {id}")

    # Create the final dataframe
    final = pd.DataFrame(final_data, columns=['NCI_NUMBER', 'MRN', 'CANCER_TYPE', 'STAGE', 'RECEPTOR_VALUE'])

    # Write the final dataframe to a CSV file
    final.to_csv('matches.csv', index=False)




if __name__ == '__main__':
    
    trialres()
    
    patres()
    
    match()
