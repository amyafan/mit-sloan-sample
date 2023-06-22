"""
Amy Fan 2-2022

This file calculates the percentage of different kinds of tradelines that are joint

input: 
_____

    ~/projects/equifaxmacro_proj/EquiFax3/2_10_Percent_Sample_Construction/output/sample/{YEARMON}_10perc.parquet
    
        10 percent sample of the full dataset 


output: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/4_Summary_Statistics/output/8_joint_tradelines.csv
    
        csv with information about the percentage of joint tradelines by type of debt 
        
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

# function to recode the equifax product categories
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
cols = ['product_category', 'ecoa']

end = 195 # 195

for filen in range(1, end + 1): 

    #######################
    # READING IN THE FILE #
    #######################
    
    DATE = monyr(filen)

    infilestr = sample + DATE + "_10perc.parquet"
    mon = pd.read_parquet(infilestr, columns = cols)
    
    # recode the product categories 
    mon = recode_nyfrb(mon)
    
    #################################
    # DROP HALF OF JOINT TRADELINES #
    #################################
    
    ind = mon[mon.ecoa != "J"]
    joint = mon[mon.ecoa == "J"]
    joint = joint.iloc[0::2,:]

    mon = pd.concat([ind, joint])
    
    ##############
    # STATISTICS #
    ##############

    # number of tradelines
    tl = mon.groupby('product_category').count()

    # number of joint tradelines 
    jtl = mon[mon.ecoa=="J"].groupby('product_category').count()
    
    #########################
    # TABULATING THE THINGS #
    #########################
    
    row = jtl/tl
    row.index = row.index + "_perc_joint"
    
    ################################################## 
    # FINALLY CONCATENATING AND EXPORTING EVERYTHING #
    ##################################################
    
    rowlist.append(row)
    
    print(f"Done with month {filen}: {DATE}", flush = True)

# finally concatenating everything 
final = pd.concat(rowlist, axis = 1).T
final.index = [monyr(num) for num in range(1, end + 1)]

# write to csv 
outfilestr = output_4 + "8_joint_tradelines.csv"

final.to_csv(outfilestr)

print('WE DID IT! :D')