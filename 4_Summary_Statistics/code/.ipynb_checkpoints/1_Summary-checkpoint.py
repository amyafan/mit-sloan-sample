"""
Amy Fan 1-2022

This calculates a bunch of summary statistics for the 10% sample using the balances_in_and_out file 

input: 
_____

    ~/projects/equifaxmacro_proj/EquiFax3/3_Balance_In_And_Out_Construction/output/1_balance_in_and_out.parquet
    
        file that tracks the balance of each tradeline for each month, as well as some other summary statistics. 


output: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/4_Summary_Statistics/output/1_summary_statistics.csv
    
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

# putting all the summary statistics in one file so that 
finalsheet = []

#######################
# READING IN THE FILE #
#######################

infilestr = output_3 + "1_balance_in_and_out.parquet"
df = pd.read_parquet(infilestr)

# we don't need the last few columns for now 
df = df.drop(['count', 'first_y', 'first_m', 'first_c','last_y', 'last_m', 'last_c', 'length'], axis = 1).reset_index()

#######################################
# BALANCE PERCENTILES BY TYPE OF DEBT #
#######################################

# calculate the amount that each person has of each kind of debt. if someone doesn't have that kind of debt, this will equal NaN
by_pc = df.drop('trade_id', axis = 1).groupby(['consumer_id', 'product_category']).sum(min_count=1)

# calculate summary statistics based off all these people by their types of debt 
balance = by_pc.reset_index().drop('consumer_id', axis=1).groupby('product_category').describe()

# The calculations are done. Now, we'll loop through each kind of debt to format the results for export 

typelist = []

for nrow in range(0, len(balance)):
    
    row = balance.iloc[nrow, :].reset_index()
    type = balance.index[nrow]
    
    temp = pd.pivot(row, index='level_0', columns='level_1', values=type).reset_index()[['count','25%', '50%', '75%',  'mean']]
    
    temp.columns = type + "_" + temp.columns
                    
    typelist.append(temp)
    
# concat all the types together and append to the list 
pc_bal = pd.concat(typelist, axis = 1)
pc_bal.index = by_pc.columns
finalsheet.append(pc_bal)

#########################
# TRADELINES PER PERSON #
#########################

# count the number of tradelines someone has 
ntl = df.groupby('consumer_id').count()

# if someone has no tradelines in a month, we don't want to consider them 
ntl = ntl.replace(0, np.NaN)

# calculate summary statistics
tradelines = ntl.describe().T.iloc[2:,:][['mean', '25%', '50%', '75%']]

# format columns and append 
tradelines.columns = "tl_" + tradelines.columns
finalsheet.append(tradelines)

#################################
# PRODUCT CATEGORIES PER PERSON #
#################################

# count number of product category types someone has 
pc_types = by_pc.groupby('consumer_id').count()

# if someone has no product categories of a type, we don't want to include them in the count for that month
pc_types = pc_types.replace(0, np.NaN)

# calculate summary statistics 
pc = pc_types.describe().T[['count', 'mean', '25%', '50%', '75%']]

# format columns and append 
pc.columns = "pc_" + pc.columns
finalsheet.append(pc)

######################
# BALANCE PER PERSON #
######################

# sum of all the balances someone has. If someone does not have any tradelines, their balance is zero
total = df.drop('trade_id', axis=1).groupby('consumer_id').sum(min_count=1)
tb = total.describe().T[['mean', '25%', '50%', '75%']]

# format columns and append
tb.columns = "totalbal_" + tb.columns
finalsheet.append(tb)

###############################################
# PERCENTAGE OF PEOPLE WITH EACH KIND OF DEBT #
###############################################

# divide the number of people with tradelines by the total number of people
perc = (pc_bal[balance.index + "_count"].T/pc['pc_count']).T

# format columns and append 
perc.columns = balance.index + "_perc"
finalsheet.append(perc)

##############################################
# NUMBER OF TRADELINES FOR EACH KIND OF DEBT #
##############################################

# count the number of tradelines for each kind of debt
tl_counts = df.groupby('product_category').count().T.iloc[2:, :]

# format columns and append
tl_counts.columns = tl_counts.columns + "_tl"
finalsheet.append(tl_counts)

################################################## 
# FINALLY CONCATENATING AND EXPORTING EVERYTHING #
##################################################

# concatenate everything 
final = pd.concat(finalsheet, axis = 1)

# write to csv 
outfilestr = output_4 + "1_summary_statistics.csv"
final.to_csv(outfilestr)

print('WE DID IT! :D')