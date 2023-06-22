"""
Amy Fan 2-2022

Calculating the number of people with mortgages and the number of mortgages that enter and exit each month 

inputs: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/3_Balance_In_And_Out_Construction/output/1_balance_in_and_out.parquet
    
        file that tracks the balance of each tradeline for each month, as well as some other summary statistics. 


outputs: 
_______

    ../output/1_mortgages_entr_exit.csv
    
        Tracks the number of mortgages that enter and exit each month    
        
"""

##############
# SETTING UP #
##############

import pandas as pd
import numpy as np

output_3 = "..//..//3_Balance_In_And_Out_Construction//output//"
output_5 = "..//output//"

######################
# READING IN DATASET #
######################

cols = ['first_c', 'last_c']

biao_str = output_3 + "1_balance_in_and_out.parquet"

biao = pd.read_parquet(biao_str, columns = cols) 

biao = biao.reset_index()
biao = biao[biao.product_category == "FM"]

final = pd.concat([biao.first_c.value_counts(), biao.last_c.value_counts()], axis = 1).sort_index() 
final['net_mort'] = final.first_c - final.last_c

final.columns = ['mort_enter', 'mort_exit', 'net_mort']

#############
# EXPORTING #
#############

final_str = output_5 + "1_mortgages_entr_exit.csv"
final.to_csv(final_str)

print("WE DID IT!!!!!!!!111!!1111!1ONEONE")