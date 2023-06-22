"""
Amy Fan 1-2022

This calculates the number of people who enter each year

input: 
_____

    ~/projects/equifaxmacro_proj/EquiFax3/3_Balance_In_And_Out_Construction/output/1_balance_in_and_out.parquet
    
        file that tracks the balance of each tradeline for each month, as well as some other summary statistics. 


output: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/4_Summary_Statistics/output/4_new_consumers.csv
    
        csv with a lot of the summary statistics. Will be copied pasted into the results spreadsheet

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

output_3 = "~//projects//equifaxmacro_proj//EquiFax3//3_Balance_In_And_Out_Construction//output//"
output_4 = "~//projects//equifaxmacro_proj//EquiFax3//4_Summary_Statistics//output//"

#######################
# READING IN THE FILE #
#######################

infilestr = output_3 + "1_balance_in_and_out.parquet"
first = pd.read_parquet(infilestr, columns = ['first_c'])

#################################### 
# DOING THE FUN DATA MANIPULATIONS #
####################################

first = first.reset_index(level = 'consumer_id')
first_ppl = first.groupby('consumer_id').min()

final = first_ppl.first_c.value_counts().sort_index()

########################################## 
# CONCATENATING AND EXPORTING EVERYTHING #
##########################################

# write to csv 
outfilestr = output_4 + "4_new_consumers.csv"
final.to_csv(outfilestr)

print('WE DID IT! :D')