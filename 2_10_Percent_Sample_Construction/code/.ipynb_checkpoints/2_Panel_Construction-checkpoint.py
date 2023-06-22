"""

Amy Fan 1-2022

Pulling together the panel based on the 10% of consumer ids. This pulls the file for one month (see shell script for job array info)


inputs: 
______    
        
    ~/projects/equifaxmacro_proj/EquiFax3/2_10_Percent_Sample_Construction/output/1_sample_ids.parquet

        Consumer IDs of the 10% 
    
    /nfs/sloanlab001/data/EquiFax3/raw/analyticsADX/{YEARMONTH}/*.csv.gz

        files for that year and month
        
    
outputs:
______

    
    ~/projects/equifaxmacro_proj/EquiFax3/2_10_Percent_Sample_Construction/output/sample/{YEARMONTH}_10perc.parquet

        all the tradelines that belong to people in 1_sample_ids.parquet. Each month is one file 


"""

import os
import pandas as pd
import numpy as np

output = "..//output//"
rawdata = "//nfs//sloanlab001//data//EquiFax3//raw//analyticsADX//"

JobID = int(os.environ['SLURM_ARRAY_TASK_ID']);

# function to convert JobID to month and year 

def monyr(monnum):

    mon = (monnum + 5) % 12 + 1

    if mon < 10: 
        MONNUM = "0" + str(mon)
    else: 
        MONNUM = str(mon)

    YRNUM = int(((monnum - mon)/12) + 2006)

    DATE = str(YRNUM) + MONNUM
    
    return DATE;

# convert JobID 
DATE = monyr(JobID)

print(f"** This is job {JobID}: {DATE} **")

##############
# READING IN #
##############

# This is the 10% of ids that we're pulling observations from 

infilestr = output + "1_sample_ids.parquet"

ids = pd.read_parquet(infilestr)

print("Read all the IDs in")

###################
# PULLING THE 10% #
###################

subfolder = rawdata + DATE + "//"

csize = 4 * (10 ** 6) # 4 million rows -- this is a bit over 10% of the dataset

count = 0 

chunklist = []

for file in os.listdir(subfolder):
    
    fileloc = subfolder + file
    
    print(f"\nFile number {count}")
    
    nchunk = 0
    
    # read in the first row to see how many columns there are 
    with pd.read_csv(fileloc, header = None, chunksize = csize) as reader:
        for chunk in reader:
            temp = chunk[chunk[0].isin(ids['consumer_id'])]
            
            chunklist.append(temp)
            
            nchunk = nchunk + 1
            
            print(f"Done with chunk {nchunk}")
            
    count = count + 1
    
# concatenate all the chunks into one big dataframe 

final = pd.concat(chunklist)
final.columns = ['consumer_id',
'archive_date',
'zip_code',
'inquiries_12_months',
'age_oldest_account',
'age_oldest_mortgage_account',
'age_newest_account',
'number_of_accounts',
'number_accounts_opened_within_12_months',
'number_accounts_always_satisfactory',
'number_accounts_major_derogatory',
'number_revolving_accts_greater_than_or_equal_to_50_percent_utilization',
'bankcard_accts_over_75_percent_utilization',
'number_accounts_past_due',
'total_past_due_amount',
'bankruptcy_flag',
'foreclosure_flag',
'number_3rd_party_collection_accts',
'total_amount_3rd_party_collections',
'number_open_bankcard_accounts',
'number_open_mortgage_accounts',
'vantage_score_3',
'state',
'trade_id',
'origination_date_open',
'origination_portfolio_type',
'origination_product_category',
'origination_vantage_score3',
'product_category',
'small_business_owner_flag',
'pim_score',
'consumer_age',
'mortgage_indicator',
'deceased_consumer',
'terms',
'status_category',
'balance',
'high_credit',
'monthly_payment',
'portfolio_type',
#'transferred_sold_flag',
'new_origination_flag',
'date_reported',
'narrcode_1',
'narrcode_2',
'narrcode_3',
'narrcode_4',
'ecoa',
'rate_status',
'scheduled_payment_amount',
'date_of_last_activity',
'date_of_last_payment',
'actual_payment_amount',
'payment_frequency',
'account_type',
'activity_designator',
'origination_vantage_score4',
'origination_Bankruptcy_Navigator_Index',
'origination_Consumer_Income_Score',
'vantage_score_4',
'bankruptcy_Navigator_Index',
'consumer_Income_Score',
'revolver_Transactor_Behavior_Last_6_Months',
'revolver_Transactor_Behavior_Last_12_Months',
'revolver_Transactor_Behavior_Last_24_Months',
'percent_Actual_Payment_to_Scheduled_Payment_Auto_Last_6_Months',
'percent_Actual_Payment_to_Scheduled_Payment_Auto_Last_12_Months',
'percent_Actual_Payment_to_Scheduled_Payment_Auto_Last_24_Months',
'percent_Actual_Payment_to_Scheduled_Payment_Mortage_Last_6_Months',
'percent_Actual_Payment_to_Scheduled_Payment_Mortage_Last_12_Months',
'percent_Actual_Payment_to_Scheduled_Payment_Mortage_Last_24_Months',
'mortgage_inquiries_last_1_month',
'potential_Mortgage_inquiries_last_1_month',
'origination_Industry_code',
'industry_code',
'consumer_age_archive',
'joint_Holders_In_Set',
'weight',
'm_y', # not included
'csv_name', # not included 
'id_num'] # not included

###########################
# WRITE THE FINAL PARQUET #
###########################

final['rate_status'] = final['rate_status'].astype(str)
final['account_type'] = final['account_type'].astype(str)

outfilestr = output + "sample//" + DATE + "_10perc.parquet"
final.to_parquet(outfilestr)