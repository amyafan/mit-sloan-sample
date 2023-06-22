"""
Amy Fan 1-2022

Replicating the NY Fed graphs on page 3 and 4 of this report: https://www.newyorkfed.org/medialibrary/interactives/householdcredit/data/pdf/HHDC_2021Q3.pdf

UPDATE 2-2022: The graph on page 3 is not accurate because it oversamples joint tradelines See 3_NY_Fed_Balances_Others for updated values 

inputs: 
______

    ~/projects/equifaxmacro_proj/EquiFax3/3_Balance_In_And_Out_Construction/output/1_balance_in_and_out.parquet
    
        file that tracks the balance of each tradeline for each month, as well as some other summary statistics. 


outputs: 
_______

    ~/projects/equifaxmacro_proj/EquiFax3/4_Summary_Statistics/output/2_total_balances.csv
    
        total balances by type of debt by quarter
    
     ~/projects/equifaxmacro_proj/EquiFax3/4_Summary_Statistics/output/2_total_accounts.csv
         
        number of accounts by quarter 

        
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
quartercols = [monyr(3*quarternum) for quarternum in range(1, 66)]

####################################
# READING IN QUARTERLY INFORMATION #
####################################

alltl_str = output_3 + "1_balance_in_and_out.parquet"
alltl = pd.read_parquet(alltl_str, columns = quartercols).reset_index()

# The kinds of debt that the NY Fed tracks, mapped to our product categories. 
# everything else will go into an "other" category

non_other_pc_types = [
            'FM', # first mortgage
            'HR', # home equity revolving 
            'AB2', # auto bank loan
            'AF2', # auto finance loan
            'BC', # bank card 
            'RT', # retail 
            'SL1', # student loan deferred
            'SL2', # student loan non-deferred  
            ]

# split into other and non-other 
non_other = alltl[alltl.product_category.isin(non_other_pc_types)]
other = alltl[~alltl.product_category.isin(non_other_pc_types)]

# now, we'll work on the non_other tradelines first. We'll recode the equifax product categories for the non-others 

# mortgage 
non_other.loc[non_other['product_category'] == 'FM', 'product_category'] = 'first_mortgage'

# home equity revolving 
non_other.loc[non_other['product_category'] == 'HR', 'product_category'] = 'he_revolving'

# auto loan 
non_other.loc[non_other['product_category'].isin(['AB2', 'AF2']), 'product_category'] = 'auto'

# credit card
non_other.loc[non_other['product_category'].isin(['BC', 'RT']), 'product_category'] = 'credit_card'

# student loan
non_other.loc[non_other['product_category'].isin(['SL1', 'SL2']), 'product_category'] = 'student_loan'

########################################################################
# OUTPUT 1: TOTAL BALANCE FOR EACH QUARTER, BROKEN OUT BY TYPE OF DEBT #
########################################################################

###############
# AGGREGATION #
###############

# non-other 
sum_non_other = non_other.drop(['consumer_id', 'trade_id'], axis = 1).groupby('product_category').sum()

# other
sum_other = other.iloc[:, 3:].sum()

# formatting to be nice 
sum_other = pd.DataFrame(sum_other).T
sum_other.index = ['other']

#################
# CONCATENATION #
#################

balances_total = pd.concat([sum_non_other, sum_other], axis = 0).T

# rename columns 
balances_total.columns = balances_total.columns + "_bal"

#############
# EXPORTING #
#############

balances_str = output_4 + "2_total_balances.csv"
balances_total.to_csv(balances_str)


###################################################################################
# OUTPUT 2: TOTAL NUMBER OF ACCOUNTS FOR EACH QUARTER, BROKEN OUT BY TYPE OF DEBT #
###################################################################################

############### 
# AGGREGATION #
###############

# we only care about this for certain types of tradelines, not the "others"

acc = non_other.drop(['consumer_id', 'trade_id'], axis = 1).groupby('product_category').count().T

# we don't care about student loan accounts 
acc = acc.drop('student_loan', axis = 1)

# rename columns 
acc.columns = acc.columns + "_acc"

#############
# EXPORTING #
#############

acc_str = output_4 + "2_total_accounts.csv"
acc.to_csv(acc_str)

print("WE DID IT!!!!!!!!111!!1111!1ONEONE")