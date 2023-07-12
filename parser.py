"""
parser.py

VERSION: 0.1

INPUTS
======
- NCI Site Identifiers
- CTRP API Key

OUTPUTS
=======
- tabulary data of trials in the Structured Eligibility Criteria (SEC) format

STEPS
=====

For Version 0.1:
1. Manually created file of trial ids at Columbia, Mount Sinai, and Montefiore (DONE)

TODO For Version 0.2:
{1. Query CTRP API for active trials at Columbia, Mount Sinai, and Montefiore
    a. filter out non-cancer trials
    b. filter for trials for breast, liver, prostate}

2. Download the JSON trial description for each active and filtered trial (DONE)

3. Parse JSON for the elibility criteria (see jupyter notebooks) (DONE)
    a. Identify Cancer Type from the anatomic sites section, map to SEC
    b. Identify Stage from the diseases section, map to SEC
    c. Identify Receptor Status from the biomarkers section, map to SEC
    d. Use text processing of semi-structured inclusion criteria for mentions of
       known cacner therapeutics.

4. Compile the output data and write to file/database store in SEC format
    a. tabulary data where each row is a trial
    b. columns:
        - trial_id
        - cancer_type
        - stage
        - receptor_status
        - prior_therapies

to do - add post-processing to eg breast to exclude stage III if also has stage IV

"""
import os
import csv
import sys
import json
import time
import sqlite3
import requests
import json
import argparse

from datetime import datetime
from collections import defaultdict

def get_trial_json(trial_id, base_url, header):
    success = True
    error_flag = '_complete'

    criteria = []
    criteria = criteria + [f'nci_id={trial_id}']
    criteria = '&'.join(criteria)
    url = base_url + criteria
    try:
        r = requests.get(url, headers=header, timeout=30)
    except Exception as e:
        print(trial_id, "straight up timeout")
        print(e)
        error_flag = "_error"
        success = False
    print(trial_id, end='\t')
    print(r, end='\t')
    if r.status_code != 200:
        print(r.status_code)
        print(r.content)
        error_flag = "_error"
        success = False
    r = r.json()

    total = r.get('total')
    data = r.get('data')


    if data is None:
        data = r
        error_flag = "_error"
        print(data)
        print(r.headers)
    else:
        try:
            pass
            data[0].pop('sites')
        except Exception as e:
            print(data, end='\t')
            print("no data or wrong data element")
            error_flag = "_error"
            success = False

    if success:
        print(error_flag)

    with open(f'./ref/trial_json/{trial_id}{error_flag}.json', 'w') as out:
        out.write(json.dumps(data, indent=4))

    time.sleep(2.5)
    return success

def load_trials(ctrp_api_key):
    CTRP_HEADERS = {'x-api-key': ctrp_api_key}

    trial_fn = './ref/trial_identifiers.txt'
    trial_fh = open(trial_fn)
    trial_ids = [line.strip() for line in trial_fh if not line.strip().startswith('#')]
    #print(trial_ids)
    print(f"Found {len(trial_ids)} trials to pull json data for.")

    # dev guide - https://clinicaltrialsapi.cancer.gov/developer-guide
    # api docs - https://clinicaltrialsapi.cancer.gov/doc
    # single trial - https://www.cancer.gov/about-cancer/treatment/clinical-trials/search/v?id=NCI-2015-00054&loc=0&rl=2&t=C4872
    # omop clinical trial extension - https://ascopubs.org/doi/full/10.1200/CCI.20.00079

    CTRP_BASE_URL = 'https://clinicaltrialsapi.cancer.gov/api/v2/trials?'

    print("Looking for existing json downloads first...")

    completed_trials = [fn.split('_')[0] for fn in os.listdir('./ref/trial_json')]
    print(f"Found {len(completed_trials)} trials where we already have json files downloaded. We will not re-download these.")

    for trial_id in trial_ids:
        if trial_id in completed_trials:
            continue

        get_trial_json(trial_id, CTRP_BASE_URL, CTRP_HEADERS)

def parse_trials():

    VALID_SITES = ["Breast",] # "Prostate", "Liver"

    # get the json files that were successfully downloaded
    files_in = [fn for fn in os.listdir('./ref/trial_json') if fn.split('_')[1].split('.')[0] == 'complete']

    print(f"Found {len(files_in)} trials with valid json.")

    trials2anatomicsites = defaultdict(set)
    anatomic_sites = defaultdict(int)
    site2trials = defaultdict(set)

    print(f"Extracting anatomic sites for each trial.")

    for fn in files_in:
        trial_id = fn.split('_')[0]
        data = json.loads(open(os.path.join('ref', 'trial_json', fn)).read())[0]
        anatomic_site = data.get('anatomic_sites')
        for site in anatomic_site:
            anatomic_sites[site] += 1
            site2trials[site].add(trial_id)
            trials2anatomicsites[trial_id].add(site)

    for site, count in anatomic_sites.items():
        print(f"\tSite: {site}, Count: {count}")

    print(f"Extracting stage inclusion criteria for each trial.")

    trials2stages = defaultdict(set)
    stages = defaultdict(int)

    for fn in files_in:
        trial_id = fn.split('_')[0]
        # print(f"Trial: {trial_id}", end="\t")
        data = json.loads(open(os.path.join('ref', 'trial_json', fn)).read())[0]
        disease = data.get('diseases')
        for d in disease:
            type = d.get('type')
            inclusion_indicator = d.get('inclusion_indicator')
            is_lead_disease = d.get('is_lead_disease')
            if inclusion_indicator != 'TRIAL':
                continue

            if 'stage' in type and d.get('name').lower().find('breast') != -1:
                stage = None
                if d.get('name').split()[0] == 'Stage':
                    stage = ' '.join(d.get('name').split()[:2])
                elif d.get('name').split()[0] == 'Recurrent':
                    stage = 'Recurrent'
                else:
                    print(f"Unexpected stage format: {d.get('name')}")

                if stage is not None:
                    trials2stages[trial_id].add(stage)
                    stages[stage] += 1

    for stage, count in stages.items():
        print(f"\tStage: {stage}, Count: {count}")

    print(f"Extracting receptor status criteria for each trial.")

    trial2pr_status = defaultdict(set)
    pr_statuses = defaultdict(int)

    trial2er_status = defaultdict(set)
    er_statuses = defaultdict(int)

    trial2her2_status = defaultdict(set)
    her2_statuses = defaultdict(int)

    trial2genes_status = defaultdict(set)
    gene_statuses = defaultdict(int)
    
    for fn in files_in:
        trial_id = fn.split('_')[0]
        # print(f"Trial: {trial_id}")
        data = json.loads(open(os.path.join('ref', 'trial_json', fn)).read())[0]
        biomarkers = data.get('biomarkers')
        if biomarkers is None:
            continue

        
        
        for biomarker in biomarkers:
            inclusion_indicator = biomarker['inclusion_indicator']

            if biomarker['name'].find('Progesteron') != -1:
                pr_status = None
                if biomarker['name'] == 'Progesterone Receptor Positive':
                    pr_status = 'PR+'
                elif biomarker['name'] == 'Progesterone Receptor Negative':
                    pr_status = 'PR-'
                elif biomarker['name'] == 'Progesterone Receptor Status':
                    # no information here
                    pass
                else:
                    print(f"Encountered unknown progesterone status: {biomarker['name']}")

                if pr_status is not None:
                    trial2pr_status[trial_id].add(pr_status)
                    pr_statuses[pr_status] += 1

            elif biomarker['name'].find('Estrogen') != -1:
                er_status = None
                if biomarker['name'] == 'Estrogen Receptor Positive':
                    er_status = 'ER+'
                elif biomarker['name'] == 'Estrogen Receptor Negative':
                    er_status = 'ER-'
                elif biomarker['name'] == 'Estrogen Receptor Status':
                    # no information here
                    pass
                else:
                    print(f"Encountered unknown estrogen status: {biomarker['name']}")

                if er_status is not None:
                    trial2er_status[trial_id].add(er_status)
                    er_statuses[er_status] += 1

            elif biomarker['name'].find('HER') != -1:
                her2_status = None
                if biomarker['name'] == 'HER2/Neu Negative':
                    her2_status = 'HER2-'
                elif biomarker['name'] == 'HER2/Neu Positive':
                    her2_status = 'HER2+'
                elif biomarker['name'] == 'HER2/Neu Status':
                    # no information here
                    pass
                else:
                    print(f"Encountered unknown HER2 status: {biomarker['name']}")

                if her2_status is not None:
                    trial2her2_status[trial_id].add(her2_status)
                    her2_statuses[her2_status] += 1
            elif "Gene or Genome" in biomarker['semantic_types'] and "reference_gene" in biomarker['type']:
                synonyms = biomarker['synonyms']
                trial2genes_status[trial_id]|=set(synonyms)
                for synonym in synonyms:
                    gene_statuses[synonym]+=1
            else:
                # this is a recpetor we currently aren't including
                pass


    for receptor_statuses in [pr_statuses, er_statuses, her2_statuses,gene_statuses]:
        for status, count in receptor_statuses.items():
            print(f"\tReceptor Status: {status}, Count: {count}")

    print(f"Extracting prior therapy criteria for each trial.")

    # load the drug names
    drug_file = open('./ref/epic_chemotherapeutics.txt')
    drug_reader = csv.reader(drug_file, delimiter='\t')

    drugs = set([row[1] for row in drug_reader if len(row) > 1])
    drug_file.close()

    print(f"\tLoaded {len(drugs)} chemotherapeutics to look for in inclusion criteria.")

    trial2prior_drugs = defaultdict(set)
    prior_drugs_count = defaultdict(int)

    for fn in files_in:
        trial_id = fn.split('_')[0]
        data = json.loads(open(os.path.join('ref', 'trial_json', fn)).read())[0]

        eligibility = data.get('eligibility')['unstructured']
        for e in eligibility:
            #line = [e['inclusion_indicator'], e['display_order'], e['description'].replace('\n', ' ').replace('\r', '')]

            if not e['inclusion_indicator']:
                # inclusion criteria are listed as True
                # exclusion criteria are listed as False (I think)
                continue

            if len(drugs & set(e['description'].split())) > 0:
                prior_drugs = drugs & set(e['description'].split())
                for d in prior_drugs:
                    trial2prior_drugs[trial_id].add(d)
                    prior_drugs_count[d] += 1
                    if d == 'tamoxifen':
                        print(trial_id)

    for drug, count in prior_drugs_count.items():
        print(f"\tPrior drug: {drug}, Count: {count}")

    return trials2anatomicsites, trials2stages, trial2er_status, trial2pr_status, trial2her2_status, trial2prior_drugs, trial2genes_status, gene_statuses

def insert_data(trials2anatomicsites, trials2stages, trial2er_status, trial2pr_status, trial2her2_status, trial2prior_drugs, trial2genes_status, genes_statuses):

    # Connect to the database and insert the trial information
    con = sqlite3.connect('disrupt.db')
    cursor = con.cursor()
    date_parsed = datetime.now()
    trials2primarykeys = dict()

    print("Inserting trial data into disrupt.db...")
    for trial_id in trials2anatomicsites.keys():
        cursor.execute(f"insert into trial (nci_number, date_parsed) values ('{trial_id}', '{date_parsed}')")
        trials2primarykeys[trial_id] = cursor.lastrowid
        #print(f"trial_id: {trial_id}, lastrowid: {cursor.lastrowid}")

    print("Inserting trial cancer types into disrupt.db...")
    for trial_id, cancer_types in trials2anatomicsites.items():
        fk_id = trials2primarykeys[trial_id]
        for cancer_type in cancer_types:
            if not cancer_type in ('Breast','Liver','Prostate'):
                continue
            cursor.execute(f"insert into trial_cancer_type (fk_id, cancer_type) values ({fk_id}, '{cancer_type}')")

    print("Inserting trial staging data into drsrupt.db...")
    for trial_id, stages in trials2stages.items():
        fk_id = trials2primarykeys[trial_id]
        for stage in stages:
            cursor.execute(f"insert into trial_stage (fk_id, stage) values ({fk_id}, '{stage}')")

    print("Inserting trial receptor status data...")
    for receptor, trial2status in [('ER', trial2er_status), ('PR', trial2pr_status), ('HER2', trial2her2_status),('Gene',trial2genes_status)]:
        print(f" {receptor}...")
        for trial_id, statuses in trial2status.items():
            fk_id = trials2primarykeys[trial_id]
            for status in statuses:
                cursor.execute(f"insert into trial_receptor (fk_id, receptor_type, receptor_value) values ({fk_id}, '{receptor}', '{status}')")

                
    print("Insert trial treatments...")
    for trial_id, prior_drugs in trial2prior_drugs.items():
        fk_id = trials2primarykeys[trial_id]
        for drug in prior_drugs:
            cursor.execute(f"insert into trial_treatment (fk_id, treatment_type, treatment_name) values ({fk_id}, 'drug', '{drug}')")

    con.commit()
    con.close()

if __name__ == '__main__':

    parser= argparse.ArgumentParser()
    parser.add_argument("--config",type=str,required=True,help="Please specify a path to the config file")
    args = parser.parse_args()
    config = json.loads(open(args.config).read())
    
    load_trials(config['ctrp_api_key'])

    data = parse_trials()

    insert_data(*data)
