"""
Amy Fan 1-2022

Calculates summary statistics for student debt levels at each age 

input: 
_____

    ~/projects/equifaxmacro_proj/EquiFax3/2_10_Percent_Sample_Construction/output/sample/{YEARMON}_10perc.parquet
    
        10 percent sample of the full dataset 


output: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/4_Summary_Statistics/output/5_student_debt_age.csv
    
        csv with information about student debt balances by age

"""

##############
# SETTING UP #
##############

import pandas as pd
import numpy as np

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

sample = "..//..//2_10_Percent_Sample_Construction//output//sample//"
output_4 = "..//output//"

#############
# FUNCTIONS #
#############

# function to convert month number into a month and year string 

def monyr(monnum):

    mon = (monnum + 5) % 12 + 1

    if mon < 10: 
        MONNUM = "0" + str(mon)
    else: 
        MONNUM = str(mon)

    YRNUM = int(((monnum - mon)/12) + 2006)

    DATE = str(YRNUM) + MONNUM
    
    return DATE;

# function to recode the types of debt 

def recode_nyfrb(non_other): 
    # mortgage 
    non_other.loc[non_other['product_category'].isin(['FM', 'HI']), 'product_category'] = 'first_mortgage'

    # home equity revolving 
    non_other.loc[non_other['product_category'] == 'HR', 'product_category'] = 'he_revolving'

    # auto loan 
    non_other.loc[non_other['product_category'].isin(['AB2', 'AF2']), 'product_category'] = 'auto'

    # credit card
    non_other.loc[non_other['product_category'].isin(['BC', 'RT']), 'product_category'] = 'credit_card'

    # student loan
    non_other.loc[non_other['product_category'].isin(['SL1', 'SL2']), 'product_category'] = 'student_loan'
    
    non_other.loc[~non_other.product_category.isin(['first_mortgage', 'he_revolving', 'auto', 'credit_card', 'student_loan']), 'product_category'] = 'other'
    
    return non_other;

#################################
# LOOPING THROUGH THE 195 FILES #
#################################

# since concatenation apparently takes a long time 
rowlist = []

# the columns that we're using
cols = ['consumer_id', 'product_category', 'balance', 'consumer_age_archive']

end = 195 # 195

for filen in range(1, end + 1): 

    #######################
    # READING IN THE FILE #
    #######################
    
    DATE = monyr(filen)

    infilestr = sample + DATE + "_10perc.parquet"
    age = pd.read_parquet(infilestr, columns = cols)
    
    # recode the types of debt 
    age = recode_nyfrb(age)
    
    ################################
    # STATISTICS FOR STUDENT LOANS #
    ################################
   
    # filter for all student loans 
    age_sl = age[age.product_category=="student_loan"]
    
    # group by the individual (we don't separate deferred and non-deferred here)
    age_sl = age_sl.groupby('consumer_id').agg({'consumer_age_archive':'mean', 'balance':'sum'})
    
    # some people apparently fall into two age buckets in the dataset (it's a tiny number). 
    # We'll lump them in the younger bucket for now
    age_sl['consumer_age_archive'] = age_sl['consumer_age_archive'].astype(int)

    # calculate the percentiles 
    summary_age_sl = age_sl.groupby('consumer_age_archive').describe(percentiles = (0.25, 0.5, 0.75, 0.9))
    
    # formatting and rewriting the index 
    summary_age_sl.columns = summary_age_sl.columns.droplevel()
    summary_age_sl = summary_age_sl[['count','mean', '25%', '50%', '75%', '90%']]
    summary_age_sl.index = ['Unknown',
                        '18–24',
                        '25–34',
                        '35-44',
                        '45-54',
                        '55–64',
                        '65–74',
                        '75+'] 
    summary_age_sl.index = summary_age_sl.index + "_SL"
    
    # we want the percentage of each age group that has debt, not the raw number
    a = age[['consumer_id', 'consumer_age_archive']].drop_duplicates().groupby('consumer_age_archive').count()
    
    # formatting for later export
    a.index = ['Unknown',
                    '18–24',
                    '25–34',
                    '35-44',
                    '45-54',
                    '55–64',
                    '65–74',
                    '75+'] 
    a.index = a.index + "_count"
    a.columns = [DATE]
    
    # calculating the percentage of people in each age group with student loan debt 
    summary_age_sl['perc'] = summary_age_sl['count']/a[DATE].to_numpy()
    
    # reshaping and reformatting the SL summary statistics 
    sl_summary = pd.melt(summary_age_sl.reset_index(), id_vars='index', value_vars=summary_age_sl.columns)
    sl_summary['colname'] = sl_summary['index'] + "_" + sl_summary['variable']
    sl_summary = pd.DataFrame(sl_summary.set_index('colname')['value'])
    sl_summary.columns = [DATE]
    
    
    
    ################################################## 
    # FINALLY CONCATENATING AND EXPORTING EVERYTHING #
    ##################################################
    finalrow = pd.concat([a, pd.DataFrame(sl_summary)], axis = 0)
    rowlist.append(finalrow)
    
    print(finalrow)
    
    print(f"Done with month {filen}: {DATE}", flush = True)

# finally concatenating everything 
final = pd.concat(rowlist, axis = 1).T

# write to csv 
outfilestr = output_4 + "5_student_debt.csv"
final.to_csv(outfilestr)

print('WE DID IT! :D')