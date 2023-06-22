"""
Amy Fan 2-2022

Calculating the number of people with mortgages and the number of mortgages that enter and exit each month 

inputs: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/3_Balance_In_And_Out_Construction/output/1_balance_in_and_out.parquet
    
        file that tracks the balance of each tradeline for each month, as well as some other summary statistics. 


outputs: 
_______

    ../output/3_mort_bal_in_and_out.csv
    
        Tracks the number of mortgages that enter and exit each month    
        
"""

##############
# SETTING UP #
##############

import pandas as pd
import numpy as np
from pandarallel import pandarallel

output_3 = "..//..//3_Balance_In_And_Out_Construction//output//"
output_5 = "..//output//"

#############
# FUNCTIONS #
#############

# convert number to DATE string
def monyr(monnum):

    mon = (monnum + 5) % 12 + 1

    if mon < 10: 
        MONNUM = "0" + str(mon)
    else: 
        MONNUM = str(mon)

    YRNUM = int(((monnum - mon)/12) + 2006)

    DATE = str(YRNUM) + MONNUM
    
    return DATE;

######################
# READING IN DATASET #
######################

biao_str = output_3 + "1_balance_in_and_out.parquet"

biao = pd.read_parquet(biao_str, engine = 'pyarrow').reset_index()
biao = biao[biao.product_category == "FM"]

# add in first balance and last balance information 
def bal(row, refcol):

    colname = monyr(int(row[refcol]))
    return row[colname]
    
# intialize pandarallel
pandarallel.initialize(progress_bar = False) 

biao['first_bal'] = biao.parallel_apply(lambda row: bal(row, 'first_c'), axis = 1)
biao['last_bal'] = biao.parallel_apply(lambda row: bal(row, 'last_c'), axis = 1)

print('done calculating first and last balances')

first = biao[['first_c', 'first_bal']].groupby('first_c').sum().sort_index()
last = biao[['last_c', 'last_bal']].groupby('last_c').sum().sort_index()

final = pd.concat([first, last], axis = 1)

final['mort_diff'] = final['first_bal'] - final['last_bal']

#############
# EXPORTING #
#############

final_str = output_5 + "3_mort_bal_in_and_out.csv"
final.to_csv(final_str)

print("WE DID IT!!!!!!!!111!!1111!1ONEONE")