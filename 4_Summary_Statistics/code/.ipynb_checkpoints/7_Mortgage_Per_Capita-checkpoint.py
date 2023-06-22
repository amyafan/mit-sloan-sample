"""
Amy Fan 1-2022

This file adjusts for the joint mortgages (drops half of the joint mortgages) and then calculates statistics on people with mortgages

input: 
_____

    ~/projects/equifaxmacro_proj/EquiFax3/2_10_Percent_Sample_Construction/output/sample/{YEARMON}_10perc.parquet
    
        10 percent sample of the full dataset 


output: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/4_Summary_Statistics/output/7_mort_per_capita.csv
    
        csv with information about mortgages by age

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
output_3 = "..//..//3_Balance_In_And_Out_Construction//output//"

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

##############################################
# GET THE FIRST MONTH SOMEONE HAS A MORTGAGE #
##############################################

# we only want one column (plus the index) 
biao_cols = ['first_c']
biao_str = output_3 + "1_balance_in_and_out.parquet"

# read in the file
biao = pd.read_parquet(biao_str, columns = biao_cols).reset_index()

# only keep mortgage tradelines and drop irrelevant index variables
biao_mort = biao[biao.product_category == "FM"].drop(['trade_id', 'product_category'], axis = 1)

# only keep the start date of the first mortgage someone has 
biao_mort = biao_mort.sort_values('first_c').drop_duplicates('consumer_id', keep = 'first')

#################################
# LOOPING THROUGH THE 195 FILES #
#################################

# since concatenation apparently takes a long time 
rowlist = []

# the columns that we're using
cols = ['consumer_id', 'trade_id', 'product_category', 'balance', 'ecoa', 'consumer_age_archive']

end = 195 # 195

for filen in range(1, end + 1): 

    #######################
    # READING IN THE FILE #
    #######################
    
    DATE = monyr(filen)

    infilestr = sample + DATE + "_10perc.parquet"
    mon = pd.read_parquet(infilestr, columns = cols)
    
    
    ############################
    # STATISTICS FOR MORTGAGES #
    ############################
   
    # calculate the total number of people by age 
    total_ppl = mon.groupby('consumer_age_archive').apply(lambda group: len(group.consumer_id.unique()))
    
    # we want to look at people who have ever had a mortgage before that month
    biao_mort_temp = biao_mort[biao_mort.first_c <= filen].consumer_id
    
    # keep the ages of people 
    mort_ever = mon.loc[mon.consumer_id.isin(biao_mort_temp),
                        ['consumer_id', 'consumer_age_archive']].drop_duplicates().consumer_age_archive.value_counts().sort_index()
    
    # filter for mortgages
    mon = mon[mon.product_category == "FM"]

    # calculate the number of people with mortgages by age
    total_mort_ppl = mon.groupby('consumer_age_archive').apply(lambda group: len(group.consumer_id.unique()))

    # separate individual and joint (there are other categories too, which is why I'm using != "J" not == "I")
    ind = mon[mon.ecoa != "J"]
    joint = mon[mon.ecoa == "J"]
    
    # count the number of people who have joint mortgages 
    joint_ppl = joint.groupby('consumer_age_archive').apply(lambda group: len(group.consumer_id.unique()))
    
    # drop half of the joint mortgages
    joint = joint.iloc[0::2,:]

    # combine the two sides again  
    new_mort = pd.concat([ind, joint])
    
    #########################
    # TABULATING THE THINGS #
    #########################
    
    # percentage of people with mortgages 
    perc_mort = total_mort_ppl/total_ppl
    perc_mort.index = ['Unknown',
                    '18–24',
                    '25–34',
                    '35-44',
                    '45-54',
                    '55–64',
                    '65–74',
                    '75+'] 

    perc_mort.index = perc_mort.index + "_mort_perc"
    
    # percentage of people who have ever had a mortgage by age
    perc_mort_ever = mort_ever/total_ppl
    perc_mort_ever.index = ['Unknown',
                    '18–24',
                    '25–34',
                    '35-44',
                    '45-54',
                    '55–64',
                    '65–74',
                    '75+'] 

    perc_mort_ever.index = perc_mort_ever.index + "_mort_ever"
    
    # percentage of people who have ever had a mortgage, total 
    perc_mort_ever_all = pd.Series(mort_ever.sum()/total_ppl.sum())
    perc_mort_ever_all.index = ['all_mort_ever']
    
    # percentage of people with joint mortgages 
    perc_joint = joint_ppl/total_mort_ppl
    perc_joint.index = ['Unknown',
                    '18–24',
                    '25–34',
                    '35-44',
                    '45-54',
                    '55–64',
                    '65–74',
                    '75+'] 

    perc_joint.index = perc_joint.index + "_perc_joint"
    
    # mortgage amounts per capita 
    mort_per_capita = new_mort.groupby('consumer_age_archive').sum()['balance']/total_ppl
    mort_per_capita.index = ['Unknown',
                    '18–24',
                    '25–34',
                    '35-44',
                    '45-54',
                    '55–64',
                    '65–74',
                    '75+'] 

    mort_per_capita.index = mort_per_capita.index + "_mort_per_capita"
    
    # summary stats for all age groups
    finalstat = pd.DataFrame([new_mort['balance'].sum()/total_ppl.sum(), joint_ppl.sum()/total_mort_ppl.sum()])
    finalstat.index = ['all_mort_per_capita', 'all_perc_joint']
    
    row = pd.concat([perc_mort, perc_mort_ever, perc_mort_ever_all, perc_joint, mort_per_capita, finalstat])
    
    ################################################## 
    # FINALLY CONCATENATING AND EXPORTING EVERYTHING #
    ##################################################
    
    rowlist.append(row)
    
    print(f"Done with month {filen}: {DATE}", flush = True)

# finally concatenating everything 
final = pd.concat(rowlist, axis = 1).T
final.index = [monyr(num) for num in range(1, end + 1)]

# write to csv 
outfilestr = output_4 + "7_mort_per_capita.csv"

final.to_csv(outfilestr)

print('WE DID IT! :D')