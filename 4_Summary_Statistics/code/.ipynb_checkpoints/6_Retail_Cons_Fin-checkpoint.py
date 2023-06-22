"""
Amy Fan 1-2022

There's a sharp switch in the number of consumer finance and retail tradelines. We want to see if this explains the sharp changes and if dropping the tradelines that are all zeros helps

inputs: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/3_Balance_In_And_Out_Construction/output/1_balance_in_and_out.parquet
    
        file that tracks the balance of each tradeline for each month, as well as some other summary statistics. 


outputs: 
_______

    ~/projects/equifaxmacro_proj/EquiFax3/4_Summary_Statistics/output/6_retail_cons_fin.csv
    
        total balances by type of debt by quarter
    
        
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

output_3 = "..//..//3_Balance_In_And_Out_Construction//output//"
output_4 = "..//output//"

############
# FUNCTION #
############

def monyr(monnum):

    mon = (monnum + 5) % 12 + 1

    if mon < 10: 
        MONNUM = "0" + str(mon)
    else: 
        MONNUM = str(mon)

    YRNUM = int(((monnum - mon)/12) + 2006)

    DATE = str(YRNUM) + MONNUM
    
    return DATE;

# we're only going to pull every third month, since it's quarterly 
moncols = [monyr(mon) for mon in range(1, 195)]

####################################
# READING IN QUARTERLY INFORMATION #
####################################

alltl_str = output_3 + "1_balance_in_and_out.parquet"
alltl = pd.read_parquet(alltl_str, columns = moncols).reset_index()

# filter for CFR and retail debt
alltl = alltl[alltl.product_category.isin(['CFR', 'RT'])]

alltl = alltl.set_index(['consumer_id', 'trade_id', 'product_category'])

# we just want to know which ones have all zero balances
tf = (alltl > 0) * 1
tf['sum'] = tf.sum(axis = 1)

# keep the ids of the ones that aren't all zero 
tf = tf[tf['sum'] != 0] 
nonzero_ids = tf.reset_index()['trade_id']

# go back to the tradelines and pick those tradelines 
alltl = alltl.reset_index()
alltl = alltl[alltl.trade_id.isin(nonzero_ids)]

print('Nothing crashed yet')

# now, let's figure out how many of those there are each month and what their average balances look like
counts = alltl.count()[3:]

final = pd.concat([counts], axis = 1)

#############
# EXPORTING #
#############

final_str = output_4 + "6_retail_cons_fin.csv"
final.to_csv(final_str)

print("WE DID IT!!!!!!!!111!!1111!1ONEONE")